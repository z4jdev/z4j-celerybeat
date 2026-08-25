"""The adapter must refuse an unimplemented overlap policy before persisting.

This exists because the refusal had no test that could fail. Deleting both
calls left all 51 celery-beat tests green, which is the same shape as every
other guard in this cycle that looked closed and was not.

Why the refusal lives HERE rather than only in ``z4j_core``: seven rounds of
guarding the model established that a subclass can override any hook a model
offers -- validator, serializer, ``model_copy``, ``copy``, ``model_construct``,
subclass default, ``model_post_init``. Nothing inside the model is
authoritative against a caller who owns the subclass. The boundary that holds
is the one the caller does not own, and that is the adapter.

Why BOTH placements: ``sources=`` is a public constructor argument and
``DjangoCeleryBeatSource`` is an exported name, so a caller can inject their
own writable source and bypass a refusal that lives only in the built-in one.
The adapter-level check covers every source; the source-level check covers a
caller who uses the source directly.

The failure being prevented: django-celery-beat has no column for an overlap
policy, so a spec carrying ``skip`` is persisted without it and mapped back as
``allow``. The caller asked for collision prevention, was told it was applied,
and gets concurrent runs.
"""

from __future__ import annotations

import warnings
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

import pytest
from pydantic import Field
from z4j_celerybeat.scheduler import CeleryBeatSchedulerAdapter
from z4j_celerybeat.sources.dcb import DjangoCeleryBeatSource
from z4j_core.models.schedule import OverlapPolicy, Schedule, ScheduleKind


def _base() -> dict[str, Any]:
    return {
        "id": uuid4(),
        "project_id": uuid4(),
        "engine": "celery",
        "scheduler": "z4j-scheduler",
        "name": "cleanup",
        "task_name": "jobs.cleanup",
        "kind": ScheduleKind.INTERVAL,
        "expression": "5m",
        "timezone": "UTC",
        "created_at": datetime.now(UTC),
        "updated_at": datetime.now(UTC),
    }


def _smuggled(policy: OverlapPolicy) -> Schedule:
    """A Schedule carrying ``policy``, built the way a caller actually could.

    Overriding ``model_post_init`` without ``super()`` is the seventh bypass an
    external reviewer found, and it is not exotic: it is what any subclass that
    wants its own post-init does. The model cannot stop this, which is the
    whole reason the adapter checks.
    """

    class _Child(Schedule):
        overlap_policy: OverlapPolicy = Field(default_factory=lambda: policy)

        def model_post_init(self, __context: object) -> None:
            return

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        return _Child(**_base())


class _AcceptingSource:
    """A writable source injected through the public ``sources=`` argument.

    It accepts anything and reports ``allow`` back, which is exactly the silent
    non-enforcement being prevented. If the adapter ever stops refusing, this
    returns and the test fails on the returned policy rather than hanging.
    """

    def supports_writes(self) -> bool:
        return True

    async def create_schedule(self, spec: Schedule) -> Schedule:
        return Schedule(**{**_base(), "overlap_policy": OverlapPolicy.ALLOW})

    async def update_schedule(self, schedule_id: str, spec: Schedule) -> Schedule:
        return Schedule(**{**_base(), "overlap_policy": OverlapPolicy.ALLOW})


@pytest.mark.parametrize("policy", [OverlapPolicy.SKIP, OverlapPolicy.QUEUE])
def test_the_builtin_source_refuses_before_persisting(policy: OverlapPolicy) -> None:
    # __new__ so no Django is required: the refusal is the first statement of
    # each write path, deliberately, so it is reached before any other state.
    source = DjangoCeleryBeatSource.__new__(DjangoCeleryBeatSource)
    spec = _smuggled(policy)
    assert spec.overlap_policy is policy, "the smuggled spec was not built; test proves nothing"

    with pytest.raises(ValueError, match="not implemented"):
        source._create_schedule_sync(spec)
    with pytest.raises(ValueError, match="not implemented"):
        source._update_schedule_sync("any-id", spec)


@pytest.mark.parametrize("policy", [OverlapPolicy.SKIP, OverlapPolicy.QUEUE])
@pytest.mark.asyncio
async def test_the_adapter_refuses_even_with_an_injected_source(
    policy: OverlapPolicy,
) -> None:
    """``sources=`` is public, so the refusal cannot live only in our source."""
    adapter = CeleryBeatSchedulerAdapter(sources=[_AcceptingSource()])
    spec = _smuggled(policy)

    with pytest.raises(ValueError, match="not implemented"):
        await adapter.create_schedule(spec)
    with pytest.raises(ValueError, match="not implemented"):
        await adapter.update_schedule("any-id", spec)


@pytest.mark.asyncio
async def test_an_ordinary_allow_schedule_is_not_blocked() -> None:
    """The refusal must not cost the supported case, which is every schedule."""
    adapter = CeleryBeatSchedulerAdapter(sources=[_AcceptingSource()])
    spec = Schedule(**_base())
    assert spec.overlap_policy is OverlapPolicy.ALLOW

    created = await adapter.create_schedule(spec)
    updated = await adapter.update_schedule("any-id", spec)
    assert created.overlap_policy is OverlapPolicy.ALLOW
    assert updated.overlap_policy is OverlapPolicy.ALLOW
