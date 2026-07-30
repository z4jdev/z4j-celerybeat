"""Tests for ``z4j_celerybeat.scheduler.CeleryBeatSchedulerAdapter``."""

from __future__ import annotations

import pytest
from z4j_celerybeat.capabilities import DEFAULT_CAPABILITIES
from z4j_celerybeat.scheduler import CeleryBeatSchedulerAdapter
from z4j_core.errors import NotFoundError
from z4j_core.models import CommandResult, Schedule, ScheduleKind
from z4j_core.protocols import SchedulerAdapter


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

    async def create_schedule(self, spec: Schedule) -> Schedule:
        raise NotImplementedError

    async def update_schedule(
        self,
        schedule_id: str,
        spec: Schedule,
    ) -> Schedule:
        raise NotImplementedError

    async def delete_schedule(self, schedule_id: str) -> CommandResult:
        return CommandResult(status="failed", error="read-only")

    async def set_enabled(
        self,
        schedule_id: str,
        enabled: bool,
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

    async def test_trigger_get_schedule_timeout_fails_closed(self, fake_celery_app) -> None:
        # G3 / celerybeat:124: get_schedule now PROPAGATES a source-read timeout.
        # trigger_now must CATCH it and refuse to fire (a raw exception would
        # escape to the dispatcher's generic internal-error handler, and firing a
        # possibly mis-identified schedule is worse). Nothing is sent.
        from z4j_celerybeat._offload import OffloadTimeoutError

        class _TimingOutGet:
            name = "dcb"

            def is_available(self) -> bool:
                return True

            async def list_schedules(self):
                return []

            async def get_schedule(self, schedule_id: str):
                raise OffloadTimeoutError("DCB get timed out")

        adapter = CeleryBeatSchedulerAdapter(celery_app=fake_celery_app, sources=[_TimingOutGet()])
        result = await adapter.trigger_now("sched-1")
        assert result.status == "failed"
        assert "could not read schedule" in (result.error or "")
        # Nothing was fired.
        assert fake_celery_app.sent_tasks == []


class _FailingSource:
    """A source whose inventory read raises (a timeout / DB error), modelling
    an AVAILABLE source that cannot complete list_schedules."""

    name = "failing"

    def __init__(self, exc: Exception) -> None:
        self._exc = exc

    def is_available(self) -> bool:
        return True

    async def list_schedules(self) -> list[Schedule]:
        raise self._exc

    async def get_schedule(self, schedule_id: str) -> Schedule | None:
        return None


class TestListPropagatesIncompleteInventory:
    """RM5: an inventory read failure must PROPAGATE, not degrade to a partial
    union. Returning only the healthy source's schedules would make the runtime
    emit an authoritative snapshot and the brain delete the failed source's
    rows. The runtime's _emit_schedule_snapshot try/excepts list_schedules and
    skips the snapshot, so propagating is the data-safe behavior."""

    async def test_source_read_failure_propagates(self) -> None:
        healthy = FakeWritableSource()
        healthy.schedules["a"] = _make_schedule("a")
        adapter = CeleryBeatSchedulerAdapter(
            sources=[healthy, _FailingSource(TimeoutError("DCB query too slow"))],
        )
        with pytest.raises(TimeoutError):
            await adapter.list_schedules()

    async def test_healthy_only_still_aggregates(self) -> None:
        # A normal source that simply returns [] must NOT trip the propagate
        # path -- only a genuine raise does.
        healthy = FakeWritableSource()
        healthy.schedules["a"] = _make_schedule("a")
        empty = FakeReadOnlySource(schedules=[])
        adapter = CeleryBeatSchedulerAdapter(sources=[healthy, empty])
        names = {s.name for s in await adapter.list_schedules()}
        assert names == {"a"}


class _IndeterminateMutationSource:
    """A writable source whose mutation TIMED OUT -> indeterminate (may have
    committed)."""

    name = "indeterminate-first"

    def is_available(self) -> bool:
        return True

    async def list_schedules(self) -> list[Schedule]:
        return []

    async def get_schedule(self, schedule_id: str) -> Schedule | None:
        return None

    async def delete_schedule(self, schedule_id: str) -> CommandResult:
        return CommandResult(status="failed", error="timed out", result={"indeterminate": True})

    async def set_enabled(self, schedule_id: str, enabled: bool) -> CommandResult:
        return CommandResult(status="failed", error="timed out", result={"indeterminate": True})


class _AlwaysSucceedsSource:
    name = "success-second"

    def is_available(self) -> bool:
        return True

    async def list_schedules(self) -> list[Schedule]:
        return []

    async def get_schedule(self, schedule_id: str) -> Schedule | None:
        return None

    async def delete_schedule(self, schedule_id: str) -> CommandResult:
        return CommandResult(status="success")

    async def set_enabled(self, schedule_id: str, enabled: bool) -> CommandResult:
        return CommandResult(status="success")


class TestIndeterminateNotBuriedBySuccess:
    """celerybeat:165: an EARLIER source's INDETERMINATE mutation (it may have
    committed) must not be buried by a LATER source's definitive success -- the
    operator still has to verify the indeterminate source."""

    async def test_delete_indeterminate_wins_over_later_success(self) -> None:
        adapter = CeleryBeatSchedulerAdapter(
            sources=[_IndeterminateMutationSource(), _AlwaysSucceedsSource()],
        )
        result = await adapter.delete_schedule("s1")
        assert result.status == "failed"
        assert result.result and result.result.get("indeterminate") is True

    async def test_set_enabled_indeterminate_wins_over_later_success(self) -> None:
        adapter = CeleryBeatSchedulerAdapter(
            sources=[_IndeterminateMutationSource(), _AlwaysSucceedsSource()],
        )
        result = await adapter.enable_schedule("s1")
        assert result.status == "failed"
        assert result.result and result.result.get("indeterminate") is True


class TestGetSchedulePropagatesTimeout:
    """celerybeat:124 +: ANY DCB get_schedule read error (a TIMEOUT or a
    ConnectionError/OperationalError) is INDETERMINATE, not absence. Falling
    through to a lower-priority source could return a DIFFERENT schedule that
    shares the id/name (a static beat entry with other args) and fire the wrong
    task, so every read error propagates (fails closed)."""

    async def test_get_schedule_timeout_propagates(self) -> None:
        from z4j_celerybeat._offload import OffloadTimeoutError

        class _TimingOutGet:
            name = "dcb"

            def is_available(self) -> bool:
                return True

            async def list_schedules(self) -> list[Schedule]:
                return []

            async def get_schedule(self, schedule_id: str) -> Schedule | None:
                raise OffloadTimeoutError("DCB get timed out")

        static = FakeReadOnlySource(schedules=[_make_schedule("sched-1")])
        adapter = CeleryBeatSchedulerAdapter(sources=[_TimingOutGet(), static])
        with pytest.raises(OffloadTimeoutError):
            await adapter.get_schedule("sched-1")

    async def test_get_schedule_non_timeout_error_propagates(self) -> None:
        # A non-timeout read error (ConnectionError/OperationalError,
        # here a bare RuntimeError) is equally INDETERMINATE. It must NOT fall
        # through to a same-named static source -- that would return a
        # mis-identified schedule and trigger_now() would fire the wrong task.
        class _BrokenGet:
            name = "broken"

            def is_available(self) -> bool:
                return True

            async def list_schedules(self) -> list[Schedule]:
                return []

            async def get_schedule(self, schedule_id: str) -> Schedule | None:
                raise RuntimeError("unexpected read error")

        static = FakeReadOnlySource(schedules=[_make_schedule("sched-1")])
        adapter = CeleryBeatSchedulerAdapter(sources=[_BrokenGet(), static])
        with pytest.raises(RuntimeError, match="unexpected read error"):
            await adapter.get_schedule("sched-1")

    async def test_trigger_now_fails_closed_on_non_timeout_read_error(self) -> None:
        # The propagated non-timeout read error must be caught by
        # trigger_now and returned as a clean, retryable failure -- never let
        # the raw exception reach the dispatcher's generic handler, and never
        # fire a mis-identified schedule.
        class _BrokenGet:
            name = "broken"

            def is_available(self) -> bool:
                return True

            async def list_schedules(self) -> list[Schedule]:
                return []

            async def get_schedule(self, schedule_id: str) -> Schedule | None:
                raise RuntimeError("db connection reset")

        static = FakeReadOnlySource(schedules=[_make_schedule("sched-1")])
        adapter = CeleryBeatSchedulerAdapter(
            sources=[_BrokenGet(), static],
            celery_app=object(),  # never reached; the read fails first
        )
        result = await adapter.trigger_now("sched-1")
        assert result.status == "failed"
        assert "refusing to fire" in (result.error or "")


class TestDcbMutationIndeterminate:
    """RM4: a DCB mutation whose broker/DB call TIMES OUT may still have
    committed, so it must report INDETERMINATE (status="failed" +
    result["indeterminate"]) rather than a clean failure the operator retries."""

    async def _timing_out_source(self, monkeypatch):
        from z4j_celerybeat._offload import OffloadTimeoutError
        from z4j_celerybeat.sources import dcb as dcb_mod

        async def _boom(*_a, **_k):
            raise OffloadTimeoutError("offload timed out")

        src = dcb_mod.DjangoCeleryBeatSource()
        monkeypatch.setattr(src, "is_available", lambda: True)
        monkeypatch.setattr(dcb_mod, "offload", _boom)
        return src

    async def test_delete_timeout_reports_indeterminate(self, monkeypatch) -> None:
        src = await self._timing_out_source(monkeypatch)
        result = await src.delete_schedule("sched-1")
        assert result.status == "failed"
        assert result.result and result.result.get("indeterminate") is True

    async def test_set_enabled_timeout_reports_indeterminate(self, monkeypatch) -> None:
        src = await self._timing_out_source(monkeypatch)
        result = await src.set_enabled("sched-1", True)
        assert result.status == "failed"
        assert result.result and result.result.get("indeterminate") is True

    async def test_create_timeout_propagates(self, monkeypatch) -> None:
        # create_schedule returns Schedule (no indeterminate CommandResult
        # channel), so a timeout PROPAGATES as the did-not-complete signal.
        from z4j_celerybeat._offload import OffloadTimeoutError

        src = await self._timing_out_source(monkeypatch)
        with pytest.raises(OffloadTimeoutError):
            await src.create_schedule(_make_schedule("new"))


class TestDeleteIndeterminateRM4:
    """RM4: a source delete that TIMED OUT (indeterminate) must survive the
    adapter's aggregation loop, not be overwritten by the generic 'not found in
    any writable source'. This drives the ADAPTER (not the bare source) --
    the operator-facing path where the earlier fix dropped the signal."""

    async def test_adapter_preserves_source_indeterminate_delete(self) -> None:
        from z4j_celerybeat._offload import indeterminate_timeout_result

        class _TimingOutDeleteSource(FakeWritableSource):
            async def delete_schedule(self, schedule_id: str) -> CommandResult:
                return indeterminate_timeout_result(
                    "delete_schedule", 15.0, hint="the schedule may still have been deleted"
                )

        adapter = CeleryBeatSchedulerAdapter(sources=[_TimingOutDeleteSource()])
        result = await adapter.delete_schedule("sched-1")
        assert result.status == "failed"
        assert result.result and result.result.get("indeterminate") is True
        assert "not found in any writable source" not in (result.error or "")

    async def test_adapter_still_reports_not_found_when_no_indeterminate(self) -> None:
        # A plain no-writable-source case is unchanged.
        adapter = CeleryBeatSchedulerAdapter(sources=[FakeReadOnlySource()])
        result = await adapter.delete_schedule("nope")
        assert result.status == "failed"
        assert "not found in any writable source" in (result.error or "")
