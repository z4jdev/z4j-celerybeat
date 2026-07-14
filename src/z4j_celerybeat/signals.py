"""Live-sync of django-celery-beat schedules to the brain.

Hooks ``post_save`` and ``post_delete`` on ``PeriodicTask`` so that
when a user creates, updates, or deletes a schedule via Django
admin (or any other code path that touches the ORM), the change
appears in the z4j dashboard within ~1 second.

Connection is OPTIONAL - only happens if django-celery-beat is
installed AND the scheduler adapter explicitly calls
:meth:`connect_signals` from its initialization. Tests can pass a
spy ``sink`` to assert the right events get emitted.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from typing import Any

from z4j_bare.safety import safe_boundary

from z4j_celerybeat.mapper import map_periodic_task

logger = logging.getLogger("z4j.adapter.celerybeat.signals")

ScheduleEventSink = Callable[[str, Any], None]
"""Callback the signals invoke when a schedule changes.

Signature: ``(action, schedule)`` where action is one of
``"created"``, ``"updated"``, ``"deleted"``. The scheduler adapter
turns these into events on the agent's outbound buffer.
"""


class CeleryBeatSignalHooks:
    """Owns the post_save / post_delete subscriptions for PeriodicTask.

    Construction is cheap. Call :meth:`connect` to subscribe and
    :meth:`disconnect` to tear down. Both are idempotent.

    Args:
        sink: Callable invoked on each create/update/delete.
        sender: Optional sender filter for ``django.dispatch.Signal``.
                Default ``None`` lets us pass the actual model class
                only when ``connect()`` runs (lazy import).
    """

    def __init__(
        self,
        *,
        sink: ScheduleEventSink,
    ) -> None:
        self.sink = sink
        self._connected = False
        self._handlers: list[tuple[Any, Callable[..., Any]]] = []

    def connect(self) -> bool:
        """Subscribe to PeriodicTask save/delete signals.

        Returns False if django-celery-beat is not installed; the
        scheduler adapter just logs a warning in that case.
        """
        if self._connected:
            return True
        try:
            from django.db.models.signals import post_delete, post_save
            from django_celery_beat.models import PeriodicTask  # type: ignore[import-not-found]
        except Exception:
            # ImportError: django or django-celery-beat not installed
            # ImproperlyConfigured: Django installed but not configured
            #   (common in Flask/FastAPI environments that share a venv)
            logger.debug(
                "django-celery-beat signal hooks unavailable; skipping",
            )
            return False

        post_save.connect(self._on_save, sender=PeriodicTask, weak=False)
        post_delete.connect(self._on_delete, sender=PeriodicTask, weak=False)
        self._handlers.append((post_save, self._on_save))
        self._handlers.append((post_delete, self._on_delete))
        self._connected = True
        logger.info("z4j celerybeat signal hooks connected")

        # Initial sync: report all existing schedules to the brain.
        # This catches schedules created before z4j was enabled.
        self._sync_existing(PeriodicTask)

        return True

    #: Initial-sync chunk size. We pace the initial-sync stream so a
    #: project with thousands of schedules does not blast the agent's
    #: bounded outbound buffer (audit medium #celerybeat-sync-flood).
    #: 100 schedules per chunk + 50 ms inter-chunk pause = ~2 k
    #: schedules/sec, well under the buffer's drain rate.
    _SYNC_CHUNK_SIZE: int = 100
    _SYNC_CHUNK_PAUSE_SECONDS: float = 0.05

    @safe_boundary
    def _sync_existing(self, periodic_task_model: Any) -> None:
        """Report all existing enabled PeriodicTask rows as 'created'.

        Uses a background thread for the Django ORM query to avoid
        SynchronousOnlyOperation when called from the async agent
        runtime. Chunked + paced so a 10 000-schedule project does
        not deluge the outbound buffer in one tick (audit
        ``celerybeat-sync-flood``).
        """
        import threading
        import time as _time

        def _do_sync() -> None:
            try:
                # ``iterator(chunk_size=...)`` streams from Django's
                # cursor without loading the full queryset into RAM.
                # ``only(...)`` keeps us from pulling JSON columns we
                # do not use during the map.
                qs = periodic_task_model.objects.filter(enabled=True).iterator(
                    chunk_size=self._SYNC_CHUNK_SIZE
                )
                count = 0
                in_chunk = 0
                for task in qs:
                    try:
                        schedule = map_periodic_task(task)
                        self.sink("created", schedule)
                        count += 1
                        in_chunk += 1
                    except Exception:  # noqa: S110  best-effort per-task sync
                        pass
                    if in_chunk >= self._SYNC_CHUNK_SIZE:
                        _time.sleep(self._SYNC_CHUNK_PAUSE_SECONDS)
                        in_chunk = 0
                if count:
                    logger.info(
                        "z4j celerybeat: synced %d existing schedules",
                        count,
                    )
            except Exception:
                logger.exception("z4j celerybeat: initial sync failed")

        thread = threading.Thread(target=_do_sync, daemon=True)
        thread.start()

    def disconnect(self) -> None:
        if not self._connected:
            return
        try:
            from django_celery_beat.models import PeriodicTask  # type: ignore[import-not-found]
        except ImportError:
            self._handlers.clear()
            self._connected = False
            return
        for signal, handler in self._handlers:
            try:
                signal.disconnect(handler, sender=PeriodicTask)
            except Exception:
                logger.exception("error disconnecting celerybeat signal handler")
        self._handlers.clear()
        self._connected = False
        logger.info("z4j celerybeat signal hooks disconnected")

    # ------------------------------------------------------------------
    # Handlers
    # ------------------------------------------------------------------

    #: Fields that Celery beat itself updates on every tick of every
    #: periodic task. A ``post_save`` whose ``update_fields`` is a
    #: subset of these is just clock noise - the schedule itself did
    #: not change. Suppressing them avoids flooding the dashboard
    #: with "schedule updated" toasts every time a beat tick fires
    #: (audit medium ``celerybeat-last-run-at-reentry``).
    _NOISE_ONLY_UPDATE_FIELDS: frozenset[str] = frozenset(
        {
            "last_run_at",
            "total_run_count",
            "date_changed",
        }
    )

    @safe_boundary
    def _on_save(
        self,
        sender: Any = None,
        instance: Any = None,
        created: bool = False,
        update_fields: Any = None,
        **_: Any,
    ) -> None:
        if instance is None:
            return
        # Suppress beat's own writebacks. ``update_fields`` is set
        # whenever Django's ``Model.save(update_fields=...)`` is
        # called explicitly - which is exactly how celery beat's
        # scheduler records the last-fire timestamp. If the only
        # changed fields are clock-noise fields, we have nothing
        # interesting to report.
        if not created and update_fields is not None:
            try:
                changed = frozenset(update_fields)
            except TypeError:
                changed = frozenset()
            if changed and changed.issubset(self._NOISE_ONLY_UPDATE_FIELDS):
                return
        try:
            schedule = map_periodic_task(instance)
        except Exception:
            logger.exception("z4j celerybeat: failed to map PeriodicTask")
            return
        action = "created" if created else "updated"
        self.sink(action, schedule)

    @safe_boundary
    def _on_delete(
        self,
        sender: Any = None,
        instance: Any = None,
        **_: Any,
    ) -> None:
        if instance is None:
            return
        try:
            schedule = map_periodic_task(instance)
        except Exception:
            logger.exception("z4j celerybeat: failed to map deleted PeriodicTask")
            return
        self.sink("deleted", schedule)


__all__ = ["CeleryBeatSignalHooks", "ScheduleEventSink"]
