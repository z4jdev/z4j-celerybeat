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

import asyncio
import json
import logging
from typing import Any

from z4j_core.errors import ConflictError, NotFoundError
from z4j_core.models import CommandResult, Schedule, ScheduleKind

from z4j_celerybeat.mapper import map_periodic_task

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
        except (ImportError, Exception):  # noqa: BLE001
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
        return await asyncio.to_thread(self._list_schedules_sync)

    def _list_schedules_sync(self) -> list[Schedule]:
        models = self._models
        rows = list(
            models.PeriodicTask.objects.select_related(
                "interval", "crontab", "clocked", "solar",
            ).all(),
        )
        return [map_periodic_task(row) for row in rows]

    async def get_schedule(self, schedule_id: str) -> Schedule | None:
        if not self.is_available():
            return None
        return await asyncio.to_thread(self._get_schedule_sync, schedule_id)

    def _get_schedule_sync(self, schedule_id: str) -> Schedule | None:
        models = self._models
        qs = models.PeriodicTask.objects.select_related(
            "interval", "crontab", "clocked", "solar",
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
        return await asyncio.to_thread(self._create_schedule_sync, spec)

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
        return await asyncio.to_thread(self._update_schedule_sync, schedule_id, spec)

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
        return await asyncio.to_thread(self._delete_schedule_sync, schedule_id)

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
        return await asyncio.to_thread(self._set_enabled_sync, schedule_id, enabled)

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
