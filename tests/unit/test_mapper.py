"""Tests for ``z4j_celerybeat.mapper``."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from z4j_core.models import Schedule, ScheduleKind
from z4j_celerybeat.mapper import map_periodic_task, map_static_entry


class TestMapPeriodicTaskInterval:
    def test_basic_interval(self) -> None:
        from tests.unit.conftest import (  # type: ignore[import-untyped]
            FakeIntervalSchedule,
            FakePeriodicTask,
        )

        periodic = FakePeriodicTask(
            name="cleanup",
            task="myapp.tasks.cleanup",
            interval=FakeIntervalSchedule(every=300, period="seconds"),
            args='["arg1"]',
            kwargs='{"k": "v"}',
            queue="maintenance",
        )
        schedule = map_periodic_task(periodic)
        assert isinstance(schedule, Schedule)
        assert schedule.name == "cleanup"
        assert schedule.task_name == "myapp.tasks.cleanup"
        assert schedule.kind == ScheduleKind.INTERVAL
        assert schedule.expression == "300"
        assert schedule.queue == "maintenance"
        assert schedule.args == ["arg1"]
        assert schedule.kwargs == {"k": "v"}

    def test_interval_minutes_to_seconds(self) -> None:
        from tests.unit.conftest import (  # type: ignore[import-untyped]
            FakeIntervalSchedule,
            FakePeriodicTask,
        )

        periodic = FakePeriodicTask(
            name="hourly",
            task="myapp.tasks.run",
            interval=FakeIntervalSchedule(every=2, period="hours"),
        )
        schedule = map_periodic_task(periodic)
        assert schedule.kind == ScheduleKind.INTERVAL
        assert schedule.expression == "7200"  # 2 hours = 7200 seconds


class TestMapPeriodicTaskCron:
    def test_basic_crontab(self) -> None:
        from tests.unit.conftest import (  # type: ignore[import-untyped]
            FakeCrontabSchedule,
            FakePeriodicTask,
        )

        periodic = FakePeriodicTask(
            name="nightly",
            task="myapp.tasks.cleanup",
            crontab=FakeCrontabSchedule(
                minute="0",
                hour="3",
                day_of_month="*",
                month_of_year="*",
                day_of_week="*",
                timezone="America/New_York",
            ),
        )
        schedule = map_periodic_task(periodic)
        assert schedule.kind == ScheduleKind.CRON
        assert schedule.expression == "0 3 * * *"
        assert schedule.timezone == "America/New_York"


class TestMapPeriodicTaskClocked:
    def test_clocked_schedule(self) -> None:
        from tests.unit.conftest import (  # type: ignore[import-untyped]
            FakeClockedSchedule,
            FakePeriodicTask,
        )

        when = datetime(2026, 4, 12, 9, 0, tzinfo=UTC)
        periodic = FakePeriodicTask(
            name="one-off",
            task="myapp.tasks.run_once",
            clocked=FakeClockedSchedule(clocked_time=when),
        )
        schedule = map_periodic_task(periodic)
        assert schedule.kind == ScheduleKind.CLOCKED
        assert "2026-04-12" in schedule.expression


class TestMapPeriodicTaskFallback:
    def test_no_related_schedule_does_not_crash(self) -> None:
        from tests.unit.conftest import FakePeriodicTask  # type: ignore[import-untyped]

        periodic = FakePeriodicTask(name="dangling", task="myapp.tasks.run")
        # No interval, no crontab, no clocked, no solar.
        schedule = map_periodic_task(periodic)
        assert schedule.kind == ScheduleKind.INTERVAL  # fallback


class TestMapPeriodicTaskJsonParsing:
    def test_handles_invalid_json_args(self) -> None:
        from tests.unit.conftest import (  # type: ignore[import-untyped]
            FakeIntervalSchedule,
            FakePeriodicTask,
        )

        periodic = FakePeriodicTask(
            name="weird",
            task="myapp.tasks.run",
            interval=FakeIntervalSchedule(),
            args="not-valid-json[",
            kwargs="also-bad{",
        )
        schedule = map_periodic_task(periodic)
        assert schedule.args == []
        assert schedule.kwargs == {}


class TestMapStaticEntry:
    def test_int_seconds(self) -> None:
        entry = {
            "task": "myapp.tasks.ping",
            "schedule": 60,
            "args": (),
            "kwargs": {"url": "https://example.com"},
        }
        schedule = map_static_entry("ping", entry)
        assert schedule.kind == ScheduleKind.INTERVAL
        assert schedule.expression == "60"
        assert schedule.task_name == "myapp.tasks.ping"

    def test_timedelta(self) -> None:
        entry = {"task": "myapp.tasks.heartbeat", "schedule": timedelta(minutes=5)}
        schedule = map_static_entry("heartbeat", entry)
        assert schedule.kind == ScheduleKind.INTERVAL
        assert schedule.expression == "300"

    def test_celery_crontab_object(self) -> None:
        class FakeCrontab:
            _orig_minute = "0"
            _orig_hour = "*/4"
            _orig_day_of_month = "*"
            _orig_month_of_year = "*"
            _orig_day_of_week = "*"
            tz = "UTC"

        entry = {"task": "myapp.tasks.every_4h", "schedule": FakeCrontab()}
        schedule = map_static_entry("every_4h", entry)
        assert schedule.kind == ScheduleKind.CRON
        assert schedule.expression == "0 */4 * * *"

    def test_static_entry_marks_metadata_source(self) -> None:
        entry = {"task": "myapp.tasks.ping", "schedule": 60}
        schedule = map_static_entry("ping", entry)
        assert schedule.metadata == {"source": "static"}

    def test_static_entry_is_enabled_by_default(self) -> None:
        entry = {"task": "myapp.tasks.ping", "schedule": 60}
        schedule = map_static_entry("ping", entry)
        assert schedule.is_enabled is True

    def test_solar_event(self) -> None:
        class FakeSolar:
            event = "sunrise"
            lat = 0.0

        entry = {"task": "myapp.tasks.dawn", "schedule": FakeSolar()}
        schedule = map_static_entry("dawn", entry)
        assert schedule.kind == ScheduleKind.SOLAR
        assert schedule.expression == "sunrise"
