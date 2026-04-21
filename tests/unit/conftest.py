"""Shared fixtures for z4j-celerybeat unit tests.

Pure Python - no Django, no django-celery-beat. The fakes here
duck-type just enough of the real classes to drive the mapper and
the static source.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

import pytest


@dataclass
class FakeIntervalSchedule:
    every: int = 60
    period: str = "seconds"


@dataclass
class FakeCrontabSchedule:
    minute: str = "0"
    hour: str = "3"
    day_of_month: str = "*"
    month_of_year: str = "*"
    day_of_week: str = "*"
    timezone: str = "UTC"


@dataclass
class FakeClockedSchedule:
    clocked_time: datetime = field(default_factory=lambda: datetime.now(UTC))


@dataclass
class FakeSolarSchedule:
    event: str = "sunrise"
    latitude: float = 0.0
    longitude: float = 0.0


@dataclass
class FakePeriodicTask:
    """Stand-in for ``django_celery_beat.models.PeriodicTask``."""

    name: str
    task: str
    args: str = "[]"
    kwargs: str = "{}"
    queue: str = ""
    enabled: bool = True
    last_run_at: datetime | None = None
    total_run_count: int = 0
    interval: FakeIntervalSchedule | None = None
    crontab: FakeCrontabSchedule | None = None
    clocked: FakeClockedSchedule | None = None
    solar: FakeSolarSchedule | None = None
    id: int = 1
    pk: int = 1
    date_changed: datetime = field(default_factory=lambda: datetime.now(UTC))


class FakeCeleryAppWithBeat:
    """Minimal Celery app exposing only ``conf.beat_schedule`` and ``send_task``."""

    def __init__(self) -> None:
        self.conf = type("Conf", (), {})()
        self.conf.beat_schedule = {}
        self.sent_tasks: list[dict[str, Any]] = []

    def send_task(
        self,
        name: str,
        *,
        args: tuple[Any, ...] | list[Any] = (),
        kwargs: dict[str, Any] | None = None,
    ) -> Any:
        self.sent_tasks.append(
            {"name": name, "args": list(args), "kwargs": dict(kwargs or {})},
        )

        class _Result:
            id = "fake-task-id"

        return _Result()


@pytest.fixture
def fake_celery_app() -> FakeCeleryAppWithBeat:
    return FakeCeleryAppWithBeat()
