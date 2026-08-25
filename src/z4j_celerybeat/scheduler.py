"""The :class:`CeleryBeatSchedulerAdapter`.

Implements :class:`z4j_core.protocols.SchedulerAdapter` for celery-beat.
Routes operations across one or more sources (django-celery-beat
ORM, static beat_schedule dict). The first source that supports the
operation wins.

Construction is cheap and synchronous; expensive availability
probes happen lazily inside the source classes.
"""

from __future__ import annotations

import logging
from typing import Any

from z4j_core.errors import NotFoundError
from z4j_core.models import (
    CommandResult,
    Schedule,
    refuse_unimplemented_overlap,
)

from z4j_celerybeat._offload import (
    OffloadTimeoutError,
    indeterminate_timeout_result,
    offload,
)
from z4j_celerybeat.capabilities import DEFAULT_CAPABILITIES
from z4j_celerybeat.signals import CeleryBeatSignalHooks
from z4j_celerybeat.sources import DjangoCeleryBeatSource, StaticBeatScheduleSource

logger = logging.getLogger("z4j.adapter.celerybeat.scheduler")

_NAME = "celery-beat"


class CeleryBeatSchedulerAdapter:
    """Scheduler adapter for celery-beat.

    Args:
        celery_app: Optional Celery application instance, used by
                    the static source to read ``conf.beat_schedule``.
                    Pass ``None`` if your project only uses
                    django-celery-beat.
        sources: Optional explicit list of sources, primarily for
                 tests that want to inject fakes. The default
                 constructs ``[DjangoCeleryBeatSource(),
                 StaticBeatScheduleSource(celery_app)]``.
    """

    name: str = _NAME

    def __init__(
        self,
        *,
        celery_app: Any | None = None,
        sources: list[Any] | None = None,
    ) -> None:
        self.celery_app = celery_app
        if sources is None:
            sources = [
                DjangoCeleryBeatSource(),
                StaticBeatScheduleSource(celery_app=celery_app),
            ]
        self.sources = sources
        self._signal_hooks: CeleryBeatSignalHooks | None = None

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def connect_signals(self, sink: Any) -> None:
        """Subscribe to django-celery-beat post_save / post_delete signals.

        Called by the agent runtime when the scheduler adapter is
        registered. ``sink`` is the runtime's event ingestion
        callback - typically a closure that builds a
        ``schedule.created`` / ``updated`` / ``deleted`` event
        and pushes it to the outbound buffer.
        """
        self._signal_hooks = CeleryBeatSignalHooks(sink=sink)
        self._signal_hooks.connect()

    def disconnect_signals(self) -> None:
        if self._signal_hooks is not None:
            self._signal_hooks.disconnect()
            self._signal_hooks = None

    # ------------------------------------------------------------------
    # SchedulerAdapter Protocol - read
    # ------------------------------------------------------------------

    async def list_schedules(self) -> list[Schedule]:
        results: list[Schedule] = []
        seen: set[tuple[str, str]] = set()
        for source in self.sources:
            try:
                items = await source.list_schedules()
            except Exception:
                # RM5: a source read that FAILED or TIMED OUT yields an
                # INCOMPLETE inventory. Swallowing it (continue) and returning
                # the partial union would make the runtime emit an AUTHORITATIVE
                # snapshot, and the brain's reconciler would DELETE every
                # schedule owned by the source that just failed. Propagate
                # instead: the runtime's _emit_schedule_snapshot try/excepts
                # list_schedules and simply skips this cycle, so nothing is
                # deleted and the next periodic resync retries. An unavailable
                # source returns [] cleanly (it does not raise), so this only
                # trips on a genuine read failure.
                logger.exception(
                    "z4j celerybeat: source %s failed to list schedules; "
                    "skipping this snapshot to avoid an authoritative partial "
                    "inventory",
                    getattr(source, "name", type(source).__name__),
                )
                raise
            for schedule in items:
                key = (schedule.scheduler, schedule.name)
                if key in seen:
                    continue
                seen.add(key)
                results.append(schedule)
        return results

    async def get_schedule(self, schedule_id: str) -> Schedule | None:
        for source in self.sources:
            try:
                schedule = await source.get_schedule(schedule_id)
            except OffloadTimeoutError:
                # celerybeat:124: a TIMEOUT is INDETERMINATE, not absence. This
                # source MAY own ``schedule_id`` -- we just could not read it in
                # time. Falling through to a lower-priority source could return a
                # DIFFERENT schedule that happens to share the id/name (a static
                # beat entry with other args), and trigger_now() would then fire
                # the wrong task. Propagate so the caller fails closed rather than
                # firing a mis-identified schedule.
                logger.warning(
                    "z4j celerybeat: source %s timed out reading schedule %s; "
                    "propagating (indeterminate, not absence)",
                    getattr(source, "name", type(source).__name__),
                    schedule_id,
                )
                raise
            except Exception:
                # celerybeat:141: ANY source-read error is
                # INDETERMINATE, not absence -- the identical reasoning to the
                # timeout branch above. A DCB source raising
                # ConnectionError/OperationalError MAY own ``schedule_id``; we
                # simply could not read it. Continuing to a lower-priority source
                # could return a DIFFERENT schedule that happens to share the
                # id/name (a static beat entry with other args), and
                # trigger_now() would then fire the WRONG task. Fail closed:
                # propagate so the caller refuses to act on a mis-identified
                # schedule rather than silently falling through.
                logger.exception(
                    "z4j celerybeat: source %s failed to get schedule %s; "
                    "propagating (indeterminate read failure, not absence)",
                    getattr(source, "name", type(source).__name__),
                    schedule_id,
                )
                raise
            if schedule is not None:
                return schedule
        return None

    # ------------------------------------------------------------------
    # SchedulerAdapter Protocol - write
    # ------------------------------------------------------------------

    async def create_schedule(self, spec: Schedule) -> Schedule:
        # Refused HERE as well as in DjangoCeleryBeatSource, because
        # ``sources=`` is a public constructor argument and
        # ``DjangoCeleryBeatSource`` is an exported name: a caller can inject
        # their own writable source, and a refusal that lives only in the
        # built-in one sits BELOW the boundary they cross. Reproduced with an
        # injected source that accepted ``skip`` and returned ``allow``.
        #
        # Both placements stay. This one covers every source; the one in the
        # built-in source covers callers who use it directly, without the
        # adapter.
        refuse_unimplemented_overlap(getattr(spec, "overlap_policy", None))

        for source in self.sources:
            if not _supports_writes(source):
                continue
            try:
                return await source.create_schedule(spec)
            except NotImplementedError:
                continue
        raise NotFoundError(
            "no scheduler source supports creating schedules; "
            "install django-celery-beat for editable schedule storage",
        )

    async def update_schedule(self, schedule_id: str, spec: Schedule) -> Schedule:
        # Refused HERE as well as in DjangoCeleryBeatSource, because
        # ``sources=`` is a public constructor argument and
        # ``DjangoCeleryBeatSource`` is an exported name: a caller can inject
        # their own writable source, and a refusal that lives only in the
        # built-in one sits BELOW the boundary they cross. Reproduced with an
        # injected source that accepted ``skip`` and returned ``allow``.
        #
        # Both placements stay. This one covers every source; the one in the
        # built-in source covers callers who use it directly, without the
        # adapter.
        refuse_unimplemented_overlap(getattr(spec, "overlap_policy", None))

        for source in self.sources:
            if not _supports_writes(source):
                continue
            try:
                return await source.update_schedule(schedule_id, spec)
            except (NotImplementedError, NotFoundError):
                continue
        raise NotFoundError(
            f"schedule {schedule_id!r} not found in any writable source",
        )

    async def delete_schedule(self, schedule_id: str) -> CommandResult:
        for source in self.sources:
            if not _supports_writes(source):
                continue
            try:
                result = await source.delete_schedule(schedule_id)
            except NotImplementedError:
                continue
            if result.status == "success":
                return result
            # RM4 + celerybeat L1: a source whose delete TIMED OUT returns
            # an INDETERMINATE failure (the delete may still have committed).
            # SHORT-CIRCUIT the source loop and surface it immediately. Trying
            # the SAME delete on the next writable source would be a second
            # mutation, and a later source raising would let the exception bury
            # this indeterminate -- the operator would then never see "verify
            # broker state; do NOT blindly retry". Returning here also means an
            # indeterminate can never be buried by a later definitive success.
            if (result.result or {}).get("indeterminate"):
                return result
        return CommandResult(
            status="failed",
            error=f"schedule {schedule_id!r} not found in any writable source",
        )

    async def enable_schedule(self, schedule_id: str) -> CommandResult:
        return await self._set_enabled(schedule_id, enabled=True)

    async def disable_schedule(self, schedule_id: str) -> CommandResult:
        return await self._set_enabled(schedule_id, enabled=False)

    async def _set_enabled(self, schedule_id: str, *, enabled: bool) -> CommandResult:
        first_failure: CommandResult | None = None
        for source in self.sources:
            # Skip sources that explicitly cannot accept writes -
            # otherwise we silently call set_enabled on a read-only
            # source which then has to manufacture a failure result
            # for every toggle command.
            if not _supports_writes(source):
                continue
            set_enabled = getattr(source, "set_enabled", None)
            if set_enabled is None:
                continue
            try:
                result = await set_enabled(schedule_id, enabled)
            except NotImplementedError:
                continue
            if result.status == "success":
                return result
            # celerybeat L1: SHORT-CIRCUIT on an INDETERMINATE toggle (the
            # source timed out and MAY have committed). Trying the SAME toggle on
            # the next writable source would be a second mutation, and a later
            # source raising would bury this indeterminate. Surfacing it now also
            # guarantees an indeterminate is never buried by a later definitive
            # success -- the operator must verify THIS source's state.
            if (result.result or {}).get("indeterminate"):
                return result
            if first_failure is None:
                first_failure = result
        if first_failure is not None:
            return first_failure
        return CommandResult(
            status="failed",
            error=f"could not toggle schedule {schedule_id!r}",
        )

    async def trigger_now(self, schedule_id: str) -> CommandResult:
        """Fire a scheduled task immediately, out-of-band.

        v1 implementation: look up the schedule, then send the task
        directly via the Celery app. This bypasses celery-beat
        entirely, which is exactly what "trigger now" should do -
        the schedule's normal cadence is unaffected.
        """
        try:
            schedule = await self.get_schedule(schedule_id)
        except Exception as exc:
            # celerybeat:124 + G3 +: get_schedule fails CLOSED on ANY
            # source-read error -- a timeout OR a ConnectionError/OperationalError
            # -- because all are INDETERMINATE, never "not found". If it fell
            # through to a same-named static-beat entry it would fire the WRONG
            # task. We could not identify the schedule, so refuse to fire. Nothing
            # was sent, so this is a clean, retryable failure (not an indeterminate
            # mutation). Catching it here also stops the raw exception from
            # reaching the dispatcher's generic "internal error" handler, which
            # would drop the fail-closed intent.
            reason = (
                "source read timed out"
                if isinstance(exc, OffloadTimeoutError)
                else f"source read failed: {exc}"
            )
            return CommandResult(
                status="failed",
                error=(
                    f"could not read schedule {schedule_id!r} to trigger it "
                    f"({reason}); refusing to fire a possibly mis-identified "
                    "schedule -- retry."
                ),
            )
        if schedule is None:
            return CommandResult(
                status="failed",
                error=f"schedule {schedule_id!r} not found",
            )
        if self.celery_app is None:
            return CommandResult(
                status="failed",
                error=(
                    "trigger_now requires a Celery app; the scheduler adapter "
                    "was constructed without one"
                ),
            )
        # send_task is a synchronous kombu broker publish; run it on the
        # dedicated broker-offload pool under a timeout so a broker slowdown /
        # failover cannot freeze the agent's single event loop OR starve its
        # heartbeat providers (isolated from the default executor). Mirrors
        # the z4j-celery action offload.
        try:
            await offload(
                self.celery_app.send_task,
                schedule.task_name,
                args=schedule.args,
                kwargs=schedule.kwargs,
                timeout=10.0,
            )
        except OffloadTimeoutError:
            # The publish may still have reached the broker; report
            # indeterminate rather than a clean failure.
            return indeterminate_timeout_result(
                "trigger_now",
                10.0,
                hint="the job may still have been enqueued",
            )
        except Exception as exc:
            return CommandResult(
                status="failed",
                error=f"send_task failed: {exc}",
            )
        return CommandResult(
            status="success",
            result={
                "schedule_id": schedule_id,
                "task_name": schedule.task_name,
            },
        )

    # ------------------------------------------------------------------
    # Capabilities
    # ------------------------------------------------------------------

    def capabilities(self) -> set[str]:
        return set(DEFAULT_CAPABILITIES)


def _supports_writes(source: Any) -> bool:
    """True if the source advertises an editable backing store."""
    is_available = getattr(source, "is_available", None)
    if is_available is None:
        return True
    try:
        return bool(is_available())
    except Exception:
        return False


__all__ = ["CeleryBeatSchedulerAdapter"]
