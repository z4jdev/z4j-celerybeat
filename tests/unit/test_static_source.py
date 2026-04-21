"""Tests for ``z4j_celerybeat.sources.static.StaticBeatScheduleSource``."""

from __future__ import annotations

import pytest

from z4j_core.models import CommandResult, Schedule, ScheduleKind
from z4j_celerybeat.sources.static import StaticBeatScheduleSource


class TestAvailability:
    def test_no_celery_app_unavailable(self) -> None:
        source = StaticBeatScheduleSource(celery_app=None)
        assert source.is_available() is False

    def test_app_with_empty_beat_unavailable(self, fake_celery_app) -> None:
        source = StaticBeatScheduleSource(celery_app=fake_celery_app)
        assert source.is_available() is False

    def test_app_with_entries_available(self, fake_celery_app) -> None:
        fake_celery_app.conf.beat_schedule = {
            "ping": {"task": "myapp.tasks.ping", "schedule": 60},
        }
        source = StaticBeatScheduleSource(celery_app=fake_celery_app)
        assert source.is_available() is True


class TestList:
    async def test_empty_returns_empty_list(self) -> None:
        source = StaticBeatScheduleSource(celery_app=None)
        assert await source.list_schedules() == []

    async def test_lists_entries(self, fake_celery_app) -> None:
        fake_celery_app.conf.beat_schedule = {
            "ping": {"task": "myapp.tasks.ping", "schedule": 60},
            "cleanup": {"task": "myapp.tasks.cleanup", "schedule": 3600},
        }
        source = StaticBeatScheduleSource(celery_app=fake_celery_app)
        schedules = await source.list_schedules()
        assert len(schedules) == 2
        names = {s.name for s in schedules}
        assert names == {"ping", "cleanup"}


class TestGet:
    async def test_get_by_name(self, fake_celery_app) -> None:
        fake_celery_app.conf.beat_schedule = {
            "ping": {"task": "myapp.tasks.ping", "schedule": 60},
        }
        source = StaticBeatScheduleSource(celery_app=fake_celery_app)
        schedule = await source.get_schedule("ping")
        assert schedule is not None
        assert schedule.task_name == "myapp.tasks.ping"

    async def test_unknown_returns_none(self, fake_celery_app) -> None:
        source = StaticBeatScheduleSource(celery_app=fake_celery_app)
        assert await source.get_schedule("nope") is None


class TestWritesUnsupported:
    async def test_create_raises(self, fake_celery_app) -> None:
        source = StaticBeatScheduleSource(celery_app=fake_celery_app)
        spec = Schedule.model_construct(
            id=None,  # type: ignore[arg-type]
            project_id=None,  # type: ignore[arg-type]
            engine="celery",
            scheduler="celery-beat",
            name="x",
            task_name="y",
            kind=ScheduleKind.INTERVAL,
            expression="60",
            timezone="UTC",
            queue=None,
            args=[],
            kwargs={},
            is_enabled=True,
            last_run_at=None,
            next_run_at=None,
            total_runs=0,
            external_id=None,
            metadata={},
            created_at=None,  # type: ignore[arg-type]
            updated_at=None,  # type: ignore[arg-type]
        )
        with pytest.raises(NotImplementedError):
            await source.create_schedule(spec)

    async def test_delete_returns_failed(self, fake_celery_app) -> None:
        source = StaticBeatScheduleSource(celery_app=fake_celery_app)
        result = await source.delete_schedule("x")
        assert isinstance(result, CommandResult)
        assert result.status == "failed"

    async def test_set_enabled_returns_failed(self, fake_celery_app) -> None:
        source = StaticBeatScheduleSource(celery_app=fake_celery_app)
        result = await source.set_enabled("x", True)
        assert result.status == "failed"
