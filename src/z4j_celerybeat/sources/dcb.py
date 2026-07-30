"""``django-celery-beat`` source.

Reads and writes ``django_celery_beat.models.PeriodicTask`` plus
its joined schedule rows. All Django ORM access happens through
``asyncio.to_thread`` because the agent runs in an asyncio loop
but Django's ORM is synchronous.

This source is OPTIONAL - if ``django_celery_beat`` is not
installed, ``is_available()`` returns False and the scheduler
adapter falls through to the next source. The agent never crashes
just because the user does not use django-celery-beat.
"""

from __future__ import annotations

import json
import logging
from typing import Any

from z4j_core.errors import ConflictError, NotFoundError
from z4j_core.models import CommandResult, Schedule, ScheduleKind

from z4j_celerybeat._offload import (
    OffloadTimeoutError,
    indeterminate_timeout_result,
    offload,
)
from z4j_celerybeat.mapper import map_periodic_task

#: M4: DCB Django-ORM I/O runs on the dedicated bounded offload pool (not
#: the shared default executor via asyncio.to_thread) under a timeout, so a
#: slow DB cannot starve the agent's heartbeat/reconnect threads. On timeout
#: offload raises OffloadTimeoutError (the op did not complete) rather than
#: hanging the loop forever.
_DCB_OFFLOAD_TIMEOUT = 15.0

logger = logging.getLogger("z4j.adapter.celerybeat.sources.dcb")


class DjangoCeleryBeatSource:
    """Source backed by django-celery-beat's ORM tables.

    Construction is cheap and lazy - :meth:`is_available` is the
    only method that touches the import. Operations grab the live
    PeriodicTask model on every call.
    """

    name: str = "django-celery-beat"

    def __init__(self) -> None:
        self._models: Any = None

    # ------------------------------------------------------------------
    # Availability probe
    # ------------------------------------------------------------------

    def is_available(self) -> bool:
        """Return True if django-celery-beat is importable and Django is configured."""
        try:
            from django_celery_beat import models  # type: ignore[import-not-found]
        except (ImportError, Exception):
            # ImportError: django-celery-beat not installed
            # Exception: Django not configured (ImproperlyConfigured) or
            #            other setup errors. Safe to catch broadly here
            #            because this is a pure availability check.
            return False
        self._models = models
        return True

    # ------------------------------------------------------------------
    # Read operations
    # ------------------------------------------------------------------

    async def list_schedules(self) -> list[Schedule]:
        if not self.is_available():
            return []
        return await offload(self._list_schedules_sync, timeout=_DCB_OFFLOAD_TIMEOUT)

    def _list_schedules_sync(self) -> list[Schedule]:
        models = self._models
        rows = list(
            models.PeriodicTask.objects.select_related(
                "interval",
                "crontab",
                "clocked",
                "solar",
            ).all(),
        )
        return [map_periodic_task(row) for row in rows]

    async def get_schedule(self, schedule_id: str) -> Schedule | None:
        if not self.is_available():
            return None
        return await offload(self._get_schedule_sync, schedule_id, timeout=_DCB_OFFLOAD_TIMEOUT)

    def _get_schedule_sync(self, schedule_id: str) -> Schedule | None:
        models = self._models
        qs = models.PeriodicTask.objects.select_related(
            "interval",
            "crontab",
            "clocked",
            "solar",
        )
        # Try by name first (brain sends schedule name), then by pk.
        try:
            row = qs.get(name=schedule_id)
        except models.PeriodicTask.DoesNotExist:
            try:
                row = qs.get(pk=int(schedule_id))
            except (models.PeriodicTask.DoesNotExist, ValueError):
                return None
        return map_periodic_task(row)

    # ------------------------------------------------------------------
    # Write operations
    # ------------------------------------------------------------------

    async def create_schedule(self, spec: Schedule) -> Schedule:
        if not self.is_available():
            raise NotFoundError(
                "django-celery-beat is not installed; cannot create schedules",
            )
        try:
            return await offload(self._create_schedule_sync, spec, timeout=_DCB_OFFLOAD_TIMEOUT)
        except OffloadTimeoutError:
            # RM4: create_schedule must return a Schedule, so it cannot carry an
            # indeterminate CommandResult; a timeout propagates as the explicit
            # "did-not-complete" signal (the reconciler retries next cycle, and
            # the create is guarded by an exists() check so a re-run is a no-op).
            # Log it so a write that later commits is not silently a bare timeout.
            logger.warning(
                "z4j celerybeat: create_schedule %r timed out after %ss; the row "
                "may still have been committed and will reconcile on the next sync",
                spec.name,
                _DCB_OFFLOAD_TIMEOUT,
            )
            raise

    def _create_schedule_sync(self, spec: Schedule) -> Schedule:
        models = self._models
        if models.PeriodicTask.objects.filter(name=spec.name).exists():
            raise ConflictError(
                f"a PeriodicTask named {spec.name!r} already exists",
            )

        related = self._build_related_schedule_object(spec)
        kwargs: dict[str, Any] = {
            "name": spec.name,
            "task": spec.task_name,
            "args": json.dumps(spec.args),
            "kwargs": json.dumps(spec.kwargs),
            "queue": spec.queue or "",
            "enabled": spec.is_enabled,
        }
        kwargs.update(related)
        row = models.PeriodicTask.objects.create(**kwargs)
        return map_periodic_task(row)

    async def update_schedule(self, schedule_id: str, spec: Schedule) -> Schedule:
        if not self.is_available():
            raise NotFoundError(
                "django-celery-beat is not installed; cannot update schedules",
            )
        try:
            return await offload(
                self._update_schedule_sync, schedule_id, spec, timeout=_DCB_OFFLOAD_TIMEOUT
            )
        except OffloadTimeoutError:
            # RM4: update_schedule must return a Schedule (no indeterminate
            # CommandResult channel); a timeout propagates as the explicit
            # "did-not-complete" signal. The update is idempotent (it re-applies
            # the same spec), so the reconciler safely retries next cycle.
            logger.warning(
                "z4j celerybeat: update_schedule %r timed out after %ss; the row "
                "may still have been committed and will reconcile on the next sync",
                schedule_id,
                _DCB_OFFLOAD_TIMEOUT,
            )
            raise

    def _resolve_periodic_task(self, schedule_id: str) -> object:
        """Look up a PeriodicTask by name or pk."""
        models = self._models
        try:
            return models.PeriodicTask.objects.get(name=schedule_id)
        except models.PeriodicTask.DoesNotExist:
            pass
        try:
            return models.PeriodicTask.objects.get(pk=int(schedule_id))
        except (models.PeriodicTask.DoesNotExist, ValueError) as exc:
            raise NotFoundError(f"schedule {schedule_id!r} not found") from exc

    def _update_schedule_sync(self, schedule_id: str, spec: Schedule) -> Schedule:
        row = self._resolve_periodic_task(schedule_id)

        # Detach old related schedule.
        for attr in ("interval", "crontab", "clocked", "solar"):
            setattr(row, attr, None)

        related = self._build_related_schedule_object(spec)
        for attr, value in related.items():
            setattr(row, attr, value)

        row.task = spec.task_name
        row.args = json.dumps(spec.args)
        row.kwargs = json.dumps(spec.kwargs)
        row.queue = spec.queue or ""
        row.enabled = spec.is_enabled
        row.save()
        return map_periodic_task(row)

    async def delete_schedule(self, schedule_id: str) -> CommandResult:
        if not self.is_available():
            return CommandResult(status="success")
        try:
            return await offload(
                self._delete_schedule_sync, schedule_id, timeout=_DCB_OFFLOAD_TIMEOUT
            )
        except OffloadTimeoutError:
            # RM4: a DELETE that exceeds the timeout may still commit on the DB.
            # Reporting a clean "failed" would invite the operator to repeat a
            # mutation whose outcome is unknown; surface it as indeterminate.
            return indeterminate_timeout_result(
                "delete_schedule",
                _DCB_OFFLOAD_TIMEOUT,
                hint="the schedule may still have been deleted",
            )

    def _delete_schedule_sync(self, schedule_id: str) -> CommandResult:
        models = self._models
        qs = models.PeriodicTask.objects.filter(name=schedule_id)
        if not qs.exists():
            qs = models.PeriodicTask.objects.filter(pk=_safe_int(schedule_id))
        deleted, _ = qs.delete()
        return CommandResult(
            status="success",
            result={"deleted": int(deleted)},
        )

    async def set_enabled(self, schedule_id: str, enabled: bool) -> CommandResult:
        if not self.is_available():
            return CommandResult(
                status="failed",
                error="django-celery-beat is not installed",
            )
        try:
            return await offload(
                self._set_enabled_sync, schedule_id, enabled, timeout=_DCB_OFFLOAD_TIMEOUT
            )
        except OffloadTimeoutError:
            # RM4: an enabled-toggle that exceeds the timeout may still commit;
            # report indeterminate rather than a definitive failure the operator
            # would blindly retry.
            return indeterminate_timeout_result(
                "set_enabled",
                _DCB_OFFLOAD_TIMEOUT,
                hint=f"the enabled={enabled} toggle may still have landed",
            )

    def _set_enabled_sync(self, schedule_id: str, enabled: bool) -> CommandResult:
        models = self._models
        qs = models.PeriodicTask.objects.filter(name=schedule_id)
        if not qs.exists():
            qs = models.PeriodicTask.objects.filter(pk=_safe_int(schedule_id))
        updated = qs.update(enabled=enabled)
        if not updated:
            return CommandResult(
                status="failed",
                error=f"schedule {schedule_id!r} not found",
            )
        return CommandResult(
            status="success",
            result={"schedule_id": schedule_id, "enabled": enabled},
        )

    # ------------------------------------------------------------------
    # Schedule-object construction
    # ------------------------------------------------------------------

    def _build_related_schedule_object(
        self,
        spec: Schedule,
    ) -> dict[str, Any]:
        """Create or fetch the IntervalSchedule / CrontabSchedule for ``spec``."""
        models = self._models
        if spec.kind == ScheduleKind.INTERVAL:
            seconds = int(spec.expression or 0)
            interval, _ = models.IntervalSchedule.objects.get_or_create(
                every=seconds,
                period=models.IntervalSchedule.SECONDS,
            )
            return {"interval": interval}

        if spec.kind == ScheduleKind.CRON:
            parts = (spec.expression or "* * * * *").split()
            while len(parts) < 5:
                parts.append("*")
            crontab, _ = models.CrontabSchedule.objects.get_or_create(
                minute=parts[0],
                hour=parts[1],
                day_of_month=parts[2],
                month_of_year=parts[3],
                day_of_week=parts[4],
                timezone=spec.timezone,
            )
            return {"crontab": crontab}

        if spec.kind == ScheduleKind.CLOCKED:
            from datetime import datetime as _dt

            try:
                clocked_time = _dt.fromisoformat(spec.expression)
            except ValueError as exc:
                raise NotFoundError(
                    f"invalid clocked expression {spec.expression!r}",
                ) from exc
            clocked, _ = models.ClockedSchedule.objects.get_or_create(
                clocked_time=clocked_time,
            )
            return {"clocked": clocked}

        if spec.kind == ScheduleKind.SOLAR:
            solar, _ = models.SolarSchedule.objects.get_or_create(
                event=spec.expression,
                latitude=0.0,
                longitude=0.0,
            )
            return {"solar": solar}

        raise NotFoundError(f"unsupported schedule kind {spec.kind!r}")


def _safe_int(value: str) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return -1


__all__ = ["DjangoCeleryBeatSource"]
