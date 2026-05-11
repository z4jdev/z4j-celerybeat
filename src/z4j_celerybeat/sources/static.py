"""Static beat-schedule source.

Reads (read-only) entries from ``celery_app.conf.beat_schedule``.
This is the dict form of beat schedules - the user hard-codes
their schedules in ``celery.py`` and Celery beat picks them up at
startup.

Static schedules cannot be modified at runtime - that's just how
Celery works. The source therefore implements only the read
operations; create/update/delete return failed
:class:`CommandResult` with a clear explanation.
"""

from __future__ import annotations

import logging
from typing import Any

from z4j_core.models import CommandResult, Schedule

from z4j_celerybeat.mapper import map_static_entry

logger = logging.getLogger("z4j.adapter.celerybeat.sources.static")


class StaticBeatScheduleSource:
    """Source for ``celery_app.conf.beat_schedule`` entries.

    Read-only. The constructor takes the live Celery app - the
    source rereads ``conf.beat_schedule`` on every call so changes
    that happen at runtime (rare) are visible.
    """

    name: str = "static"

    def __init__(self, celery_app: Any | None = None) -> None:
        self._celery_app = celery_app

    # ------------------------------------------------------------------
    # Availability
    # ------------------------------------------------------------------

    def is_available(self) -> bool:
        """True if a Celery app was supplied AND has a non-empty beat schedule."""
        if self._celery_app is None:
            return False
        try:
            beat_schedule = self._celery_app.conf.beat_schedule
        except AttributeError:
            return False
        return bool(beat_schedule)

    # ------------------------------------------------------------------
    # Read operations
    # ------------------------------------------------------------------

    async def list_schedules(self) -> list[Schedule]:
        if self._celery_app is None:
            return []
        try:
            beat_schedule = self._celery_app.conf.beat_schedule or {}
        except AttributeError:
            return []
        return [
            map_static_entry(name, dict(entry))
            for name, entry in beat_schedule.items()
        ]

    async def get_schedule(self, schedule_id: str) -> Schedule | None:
        for schedule in await self.list_schedules():
            if schedule.external_id == schedule_id or schedule.name == schedule_id:
                return schedule
        return None

    # ------------------------------------------------------------------
    # Write operations - all unsupported
    # ------------------------------------------------------------------

    async def create_schedule(self, spec: Schedule) -> Schedule:  # noqa: ARG002
        raise NotImplementedError(
            "static beat_schedule entries cannot be created at runtime; "
            "edit celery_app.conf.beat_schedule in source code instead, "
            "or install django-celery-beat for editable schedules",
        )

    async def update_schedule(  # noqa: ARG002
        self, schedule_id: str, spec: Schedule,
    ) -> Schedule:
        raise NotImplementedError(
            "static beat_schedule entries are read-only",
        )

    async def delete_schedule(self, schedule_id: str) -> CommandResult:  # noqa: ARG002
        return CommandResult(
            status="failed",
            error="static beat_schedule entries are read-only",
        )

    async def set_enabled(  # noqa: ARG002
        self,
        schedule_id: str,
        enabled: bool,
    ) -> CommandResult:
        return CommandResult(
            status="failed",
            error="static beat_schedule entries are read-only",
        )


__all__ = ["StaticBeatScheduleSource"]
