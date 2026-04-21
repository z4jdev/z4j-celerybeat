"""Translate Celery / django-celery-beat schedule shapes ↔ z4j Schedules.

The schedule data lives in three different places depending on
which backend the user runs:

1. **django-celery-beat** - rows in
   ``django_celery_beat.models.PeriodicTask`` joined with one of
   ``IntervalSchedule``, ``CrontabSchedule``, ``ClockedSchedule``,
   or ``SolarSchedule``.
2. **Static** - entries in ``celery_app.conf.beat_schedule`` (a
   plain dict).
3. **Celery's** ``celery.schedules.{crontab,schedule,solar}``
   instances when reading the in-memory ``Schedule.entries`` from
   a running ``celery beat`` process.

This module knows how to flatten all three into a single
:class:`z4j_core.models.Schedule` shape, and how to translate
:class:`Schedule` writes coming from the brain back into the
storage layer's native form.

Pure-Python: no Django imports here. The dcb source module imports
``django_celery_beat`` lazily and uses these helpers.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from uuid import UUID, uuid4

from z4j_core.models import Schedule, ScheduleKind

_ENGINE = "celery"
_SCHEDULER = "celery-beat"


def _placeholder_uuid() -> UUID:
    """Schedules need a UUID id; one is assigned at mapper time and the
    brain re-keys it on ingest. The mapper does not need to know real
    project IDs."""
    return uuid4()


# ---------------------------------------------------------------------------
# django-celery-beat row → Schedule
# ---------------------------------------------------------------------------


def map_periodic_task(periodic: Any, project_id: UUID | None = None) -> Schedule:
    """Convert one ``PeriodicTask`` row to a :class:`Schedule`.

    Accepts duck-typed input so unit tests don't need a real
    django_celery_beat installation. The minimum required attributes
    are listed in the test fixtures.
    """
    kind, expression, timezone = _resolve_kind_and_expression(periodic)
    return Schedule(
        id=_placeholder_uuid(),
        project_id=project_id or _placeholder_uuid(),
        engine=_ENGINE,
        scheduler=_SCHEDULER,
        name=str(periodic.name),
        task_name=str(periodic.task),
        kind=kind,
        expression=expression,
        timezone=timezone,
        queue=getattr(periodic, "queue", None) or None,
        args=_parse_json_field(getattr(periodic, "args", "[]")) or [],
        kwargs=_parse_json_field(getattr(periodic, "kwargs", "{}")) or {},
        is_enabled=bool(getattr(periodic, "enabled", True)),
        last_run_at=getattr(periodic, "last_run_at", None),
        next_run_at=None,  # django-celery-beat does not store this; brain computes it
        total_runs=int(getattr(periodic, "total_run_count", 0)),
        external_id=str(getattr(periodic, "id", getattr(periodic, "pk", "")) or ""),
        metadata={},
        created_at=getattr(periodic, "date_changed", None) or datetime.now(UTC),
        updated_at=getattr(periodic, "date_changed", None) or datetime.now(UTC),
    )


def _resolve_kind_and_expression(periodic: Any) -> tuple[ScheduleKind, str, str]:
    """Inspect a PeriodicTask's joined schedule object."""
    interval = getattr(periodic, "interval", None)
    if interval is not None:
        every = int(getattr(interval, "every", 0))
        period = str(getattr(interval, "period", "seconds"))
        seconds = _interval_to_seconds(every, period)
        return ScheduleKind.INTERVAL, str(seconds), "UTC"

    crontab = getattr(periodic, "crontab", None)
    if crontab is not None:
        expression = " ".join(
            [
                str(getattr(crontab, "minute", "*")),
                str(getattr(crontab, "hour", "*")),
                str(getattr(crontab, "day_of_month", "*")),
                str(getattr(crontab, "month_of_year", "*")),
                str(getattr(crontab, "day_of_week", "*")),
            ],
        )
        timezone_attr = getattr(crontab, "timezone", None)
        timezone = str(timezone_attr) if timezone_attr else "UTC"
        return ScheduleKind.CRON, expression, timezone

    clocked = getattr(periodic, "clocked", None)
    if clocked is not None:
        clocked_time = getattr(clocked, "clocked_time", None)
        expression = clocked_time.isoformat() if clocked_time else ""
        return ScheduleKind.CLOCKED, expression, "UTC"

    solar = getattr(periodic, "solar", None)
    if solar is not None:
        event = getattr(solar, "event", "")
        return ScheduleKind.SOLAR, str(event), "UTC"

    # Fallback - periodic_task is enabled but has no joined schedule.
    # This shouldn't happen with a healthy database but we don't want
    # the mapper to crash.
    return ScheduleKind.INTERVAL, "0", "UTC"


def _interval_to_seconds(every: int, period: str) -> int:
    """Convert a django-celery-beat interval to whole seconds.

    A ``microseconds`` period is the only one that can collapse to
    zero - and a zero-second interval would tell every downstream
    consumer "this schedule never fires." We round it up to a
    minimum of 1 second so the schedule still has a sensible
    representation, while preserving "this is sub-second" via the
    metadata downstream.
    """
    multipliers = {
        "seconds": 1,
        "minutes": 60,
        "hours": 3600,
        "days": 86400,
    }
    period_lc = period.lower()
    if period_lc == "microseconds":
        # Anything <1s rounds up to 1s. Truly sub-second cron-style
        # schedules are not a thing django-celery-beat supports
        # in practice (the worker tick is much slower than that).
        return max(1, (every + 999_999) // 1_000_000)
    return every * multipliers.get(period_lc, 1)


def _parse_json_field(value: Any) -> Any:
    """Parse a JSON-string field tolerantly.

    django-celery-beat stores ``args`` and ``kwargs`` as TextField
    JSON. Older rows may have invalid JSON; the user shouldn't
    care.
    """
    if value is None:
        return None
    if isinstance(value, (list, dict)):
        return value
    if not isinstance(value, str):
        return value
    text = value.strip()
    if not text:
        return None
    import json

    try:
        return json.loads(text)
    except (ValueError, TypeError):
        return None


# ---------------------------------------------------------------------------
# Static dict → Schedule
# ---------------------------------------------------------------------------


def map_static_entry(
    name: str,
    entry: dict[str, Any],
    project_id: UUID | None = None,
) -> Schedule:
    """Convert one ``celery_app.conf.beat_schedule`` entry to :class:`Schedule`.

    The entry shape is::

        {
            "task": "myapp.tasks.cleanup",
            "schedule": <crontab() | int seconds | timedelta>,
            "args": (...),
            "kwargs": {...},
            "options": {"queue": "..."},
        }
    """
    task_name = str(entry.get("task", ""))
    raw_schedule = entry.get("schedule")
    kind, expression, timezone = _resolve_static_schedule(raw_schedule)
    options = entry.get("options") or {}
    queue = options.get("queue") if isinstance(options, dict) else None

    return Schedule(
        id=_placeholder_uuid(),
        project_id=project_id or _placeholder_uuid(),
        engine=_ENGINE,
        scheduler=_SCHEDULER,
        name=name,
        task_name=task_name,
        kind=kind,
        expression=expression,
        timezone=timezone,
        queue=str(queue) if queue else None,
        args=list(entry.get("args") or []),
        kwargs=dict(entry.get("kwargs") or {}),
        is_enabled=True,  # static entries don't have an enabled flag
        last_run_at=None,
        next_run_at=None,
        total_runs=0,
        external_id=name,
        metadata={"source": "static"},
        created_at=datetime.now(UTC),
        updated_at=datetime.now(UTC),
    )


def _resolve_static_schedule(value: Any) -> tuple[ScheduleKind, str, str]:
    """Inspect ``schedule`` from a static entry."""
    if value is None:
        return ScheduleKind.INTERVAL, "0", "UTC"

    # Plain integer / float → interval seconds.
    if isinstance(value, (int, float)):
        return ScheduleKind.INTERVAL, str(int(value)), "UTC"

    # ``datetime.timedelta`` → interval seconds.
    from datetime import timedelta

    if isinstance(value, timedelta):
        return ScheduleKind.INTERVAL, str(int(value.total_seconds())), "UTC"

    # Celery ``crontab(...)`` instance: it has minute / hour / day_of_week / etc.
    if hasattr(value, "_orig_minute"):
        expression = " ".join(
            [
                _stringify_cron_field(getattr(value, "_orig_minute", "*")),
                _stringify_cron_field(getattr(value, "_orig_hour", "*")),
                _stringify_cron_field(getattr(value, "_orig_day_of_month", "*")),
                _stringify_cron_field(getattr(value, "_orig_month_of_year", "*")),
                _stringify_cron_field(getattr(value, "_orig_day_of_week", "*")),
            ],
        )
        tz = getattr(value, "tz", None)
        timezone = str(tz) if tz else "UTC"
        return ScheduleKind.CRON, expression, timezone

    # Celery ``solar(...)``
    if hasattr(value, "event") and hasattr(value, "lat"):
        return ScheduleKind.SOLAR, str(value.event), "UTC"

    # Last resort: stringify and call it cron.
    return ScheduleKind.CRON, str(value), "UTC"


def _stringify_cron_field(value: Any) -> str:
    if value is None:
        return "*"
    return str(value)


__all__ = [
    "map_periodic_task",
    "map_static_entry",
]
