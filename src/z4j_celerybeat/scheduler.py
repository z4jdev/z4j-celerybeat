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
from z4j_core.models import CommandResult, Schedule

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
            except Exception:  # noqa: BLE001
                logger.exception(
                    "z4j celerybeat: source %s failed to list schedules",
                    getattr(source, "name", type(source).__name__),
                )
                continue
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
            except Exception:  # noqa: BLE001
                logger.exception(
                    "z4j celerybeat: source %s failed to get schedule",
                    getattr(source, "name", type(source).__name__),
                )
                continue
            if schedule is not None:
                return schedule
        return None

    # ------------------------------------------------------------------
    # SchedulerAdapter Protocol - write
    # ------------------------------------------------------------------

    async def create_schedule(self, spec: Schedule) -> Schedule:
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
        schedule = await self.get_schedule(schedule_id)
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
        try:
            self.celery_app.send_task(
                schedule.task_name,
                args=schedule.args,
                kwargs=schedule.kwargs,
            )
        except Exception as exc:  # noqa: BLE001
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
    except Exception:  # noqa: BLE001
        return False


__all__ = ["CeleryBeatSchedulerAdapter"]
