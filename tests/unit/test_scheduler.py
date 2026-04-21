"""Tests for ``z4j_celerybeat.scheduler.CeleryBeatSchedulerAdapter``."""

from __future__ import annotations

import pytest

from z4j_core.errors import NotFoundError
from z4j_core.models import CommandResult, Schedule, ScheduleKind
from z4j_core.protocols import SchedulerAdapter
from z4j_celerybeat.capabilities import DEFAULT_CAPABILITIES
from z4j_celerybeat.scheduler import CeleryBeatSchedulerAdapter


class FakeWritableSource:
    """In-memory writable source for tests."""

    name = "fake-writable"

    def __init__(self) -> None:
        self.schedules: dict[str, Schedule] = {}

    def is_available(self) -> bool:
        return True

    async def list_schedules(self) -> list[Schedule]:
        return list(self.schedules.values())

    async def get_schedule(self, schedule_id: str) -> Schedule | None:
        return self.schedules.get(schedule_id)

    async def create_schedule(self, spec: Schedule) -> Schedule:
        self.schedules[spec.name] = spec
        return spec

    async def update_schedule(self, schedule_id: str, spec: Schedule) -> Schedule:
        self.schedules[schedule_id] = spec
        return spec

    async def delete_schedule(self, schedule_id: str) -> CommandResult:
        self.schedules.pop(schedule_id, None)
        return CommandResult(status="success", result={"deleted": 1})

    async def set_enabled(self, schedule_id: str, enabled: bool) -> CommandResult:
        if schedule_id not in self.schedules:
            return CommandResult(status="failed", error="not found")
        return CommandResult(
            status="success",
            result={"schedule_id": schedule_id, "enabled": enabled},
        )


class FakeReadOnlySource:
    """In-memory read-only source."""

    name = "fake-readonly"

    def __init__(self, schedules: list[Schedule] | None = None) -> None:
        self._schedules = list(schedules or [])

    def is_available(self) -> bool:
        return False  # not writable

    async def list_schedules(self) -> list[Schedule]:
        return list(self._schedules)

    async def get_schedule(self, schedule_id: str) -> Schedule | None:
        for s in self._schedules:
            if s.name == schedule_id or s.external_id == schedule_id:
                return s
        return None

    async def create_schedule(self, spec: Schedule) -> Schedule:  # noqa: ARG002
        raise NotImplementedError

    async def update_schedule(  # noqa: ARG002
        self, schedule_id: str, spec: Schedule,
    ) -> Schedule:
        raise NotImplementedError

    async def delete_schedule(self, schedule_id: str) -> CommandResult:  # noqa: ARG002
        return CommandResult(status="failed", error="read-only")

    async def set_enabled(  # noqa: ARG002
        self, schedule_id: str, enabled: bool,
    ) -> CommandResult:
        return CommandResult(status="failed", error="read-only")


def _make_schedule(name: str = "test") -> Schedule:
    from datetime import UTC, datetime
    from uuid import uuid4

    return Schedule(
        id=uuid4(),
        project_id=uuid4(),
        engine="celery",
        scheduler="celery-beat",
        name=name,
        task_name="myapp.tasks.run",
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
        external_id=name,
        metadata={},
        created_at=datetime.now(UTC),
        updated_at=datetime.now(UTC),
    )


class TestProtocolConformance:
    def test_satisfies_protocol(self) -> None:
        adapter = CeleryBeatSchedulerAdapter(sources=[])
        assert isinstance(adapter, SchedulerAdapter)

    def test_capabilities(self) -> None:
        adapter = CeleryBeatSchedulerAdapter(sources=[])
        assert adapter.capabilities() == set(DEFAULT_CAPABILITIES)


class TestList:
    async def test_empty_no_sources(self) -> None:
        adapter = CeleryBeatSchedulerAdapter(sources=[])
        assert await adapter.list_schedules() == []

    async def test_aggregates_across_sources(self) -> None:
        s1 = FakeWritableSource()
        s1.schedules["a"] = _make_schedule("a")
        s2 = FakeReadOnlySource(schedules=[_make_schedule("b")])
        adapter = CeleryBeatSchedulerAdapter(sources=[s1, s2])
        schedules = await adapter.list_schedules()
        names = {s.name for s in schedules}
        assert names == {"a", "b"}

    async def test_dedupes_by_scheduler_and_name(self) -> None:
        s1 = FakeWritableSource()
        s1.schedules["a"] = _make_schedule("a")
        s2 = FakeReadOnlySource(schedules=[_make_schedule("a")])
        adapter = CeleryBeatSchedulerAdapter(sources=[s1, s2])
        schedules = await adapter.list_schedules()
        assert len(schedules) == 1


class TestCreate:
    async def test_create_uses_writable_source(self) -> None:
        writable = FakeWritableSource()
        adapter = CeleryBeatSchedulerAdapter(sources=[writable])
        spec = _make_schedule("new")
        created = await adapter.create_schedule(spec)
        assert created.name == "new"
        assert "new" in writable.schedules

    async def test_create_skips_unavailable_sources(self) -> None:
        readonly = FakeReadOnlySource()
        writable = FakeWritableSource()
        adapter = CeleryBeatSchedulerAdapter(sources=[readonly, writable])
        spec = _make_schedule("new")
        await adapter.create_schedule(spec)
        assert "new" in writable.schedules

    async def test_create_raises_when_no_writable(self) -> None:
        readonly = FakeReadOnlySource()
        adapter = CeleryBeatSchedulerAdapter(sources=[readonly])
        with pytest.raises(NotFoundError):
            await adapter.create_schedule(_make_schedule("new"))


class TestUpdate:
    async def test_update_uses_writable(self) -> None:
        writable = FakeWritableSource()
        writable.schedules["x"] = _make_schedule("x")
        adapter = CeleryBeatSchedulerAdapter(sources=[writable])
        new_spec = _make_schedule("x")
        result = await adapter.update_schedule("x", new_spec)
        assert result.name == "x"


class TestDelete:
    async def test_delete_uses_writable(self) -> None:
        writable = FakeWritableSource()
        writable.schedules["gone"] = _make_schedule("gone")
        adapter = CeleryBeatSchedulerAdapter(sources=[writable])
        result = await adapter.delete_schedule("gone")
        assert result.status == "success"
        assert "gone" not in writable.schedules

    async def test_delete_no_writable_returns_failed(self) -> None:
        readonly = FakeReadOnlySource()
        adapter = CeleryBeatSchedulerAdapter(sources=[readonly])
        result = await adapter.delete_schedule("anything")
        assert result.status == "failed"


class TestEnableDisable:
    async def test_enable(self) -> None:
        writable = FakeWritableSource()
        writable.schedules["x"] = _make_schedule("x")
        adapter = CeleryBeatSchedulerAdapter(sources=[writable])
        result = await adapter.enable_schedule("x")
        assert result.status == "success"

    async def test_disable(self) -> None:
        writable = FakeWritableSource()
        writable.schedules["x"] = _make_schedule("x")
        adapter = CeleryBeatSchedulerAdapter(sources=[writable])
        result = await adapter.disable_schedule("x")
        assert result.status == "success"


class TestTriggerNow:
    async def test_trigger_with_celery_app(self, fake_celery_app) -> None:
        writable = FakeWritableSource()
        writable.schedules["test"] = _make_schedule("test")
        adapter = CeleryBeatSchedulerAdapter(
            celery_app=fake_celery_app,
            sources=[writable],
        )
        result = await adapter.trigger_now("test")
        assert result.status == "success"
        assert fake_celery_app.sent_tasks[0]["name"] == "myapp.tasks.run"

    async def test_trigger_unknown_schedule(self, fake_celery_app) -> None:
        adapter = CeleryBeatSchedulerAdapter(
            celery_app=fake_celery_app,
            sources=[FakeWritableSource()],
        )
        result = await adapter.trigger_now("nope")
        assert result.status == "failed"

    async def test_trigger_without_celery_app(self) -> None:
        writable = FakeWritableSource()
        writable.schedules["x"] = _make_schedule("x")
        adapter = CeleryBeatSchedulerAdapter(celery_app=None, sources=[writable])
        result = await adapter.trigger_now("x")
        assert result.status == "failed"
        assert "Celery app" in (result.error or "")
