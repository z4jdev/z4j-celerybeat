"""Exercise the adapter against real django-celery-beat models in isolation."""

from __future__ import annotations

import subprocess
import sys

import pytest


def test_native_database_operations(tmp_path):
    pytest.importorskip(
        "django_celery_beat", reason="native database integration requires the Django extra"
    )
    crontab = pytest.importorskip("crontab")
    if not hasattr(crontab, "CronSlices"):
        pytest.skip(
            "rq-scheduler's crontab conflicts with python-crontab; run scripts/test-native-beat.sh"
        )
    probe = r"""
import asyncio
import sys
from datetime import UTC, datetime, timedelta
from uuid import uuid4
from django.conf import settings

settings.configure(
    INSTALLED_APPS=["django_celery_beat"],
    DATABASES={"default": {"ENGINE": "django.db.backends.sqlite3", "NAME": sys.argv[1]}},
    USE_TZ=True, TIME_ZONE="UTC", SECRET_KEY="isolated-test",
)
import django
django.setup()
from django.core.management import call_command
call_command("migrate", verbosity=0)
from django_celery_beat.models import PeriodicTask, PeriodicTasks
from z4j_celerybeat.sources.dcb import DjangoCeleryBeatSource
from z4j_core.models import Schedule, ScheduleKind

source = DjangoCeleryBeatSource()
assert source.is_available()
now = datetime.now(UTC)
spec = Schedule(id=uuid4(), project_id=uuid4(), engine="celery", scheduler="celery-beat",
    name="original", task_name="jobs.run", kind=ScheduleKind.INTERVAL, expression="60",
    created_at=now, updated_at=now)
micro = asyncio.run(source.create_schedule(spec.model_copy(update={"name": "micro", "expression": "0.125"})))
assert micro.expression == "0.125"
assert PeriodicTask.objects.get(name="micro").interval.period == "microseconds"
original = asyncio.run(source.create_schedule(spec))
pk = int(original.external_id.removeprefix("dcb:"))
# A numeric NAME must not redirect a command addressed to the other row's ID.
collision = asyncio.run(source.create_schedule(spec.model_copy(update={"name": str(pk)})))
assert asyncio.run(source.get_schedule(original.external_id)).name == "original"
PeriodicTask.objects.filter(pk=pk).update(last_run_at=now)
before = PeriodicTasks.last_change()
assert asyncio.run(source.set_enabled(original.external_id, False)).status == "success"
assert PeriodicTasks.last_change() > before, "a running Beat must be told to reload"
row = PeriodicTask.objects.get(pk=pk)
assert not row.enabled and row.last_run_at is None
assert PeriodicTask.objects.get(name=str(pk)).enabled

clocked_spec = spec.model_copy(update={"name": "once", "kind": ScheduleKind.CLOCKED,
    "expression": (now + timedelta(days=1)).isoformat()})
clocked = asyncio.run(source.create_schedule(clocked_spec))
row = PeriodicTask.objects.get(name="once")
assert row.one_off and row.clocked_id is not None
updated = asyncio.run(source.update_schedule(clocked.external_id,
    spec.model_copy(update={"name": "renamed"})))
row.refresh_from_db()
assert row.name == "renamed" and not row.one_off and row.clocked_id is None
assert row.interval_id is not None and updated.external_id == clocked.external_id
assert asyncio.run(source.delete_schedule(original.external_id)).status == "success"
assert not PeriodicTask.objects.filter(pk=pk).exists()
assert PeriodicTask.objects.filter(name=str(pk)).exists()
from z4j_core.schedule_external import normalize_external_schedule
from z4j_core.errors import ConflictError
observed = normalize_external_schedule(updated.model_dump(mode="json"), owner="celery-beat")
changed = asyncio.run(source.apply_external_mutation(operation="update",
    schedule_id=updated.external_id, expected=observed, definition={"expression": "90"}))
assert changed.expression == "90"
try:
    asyncio.run(source.apply_external_mutation(operation="delete",
        schedule_id=updated.external_id, expected=observed, definition={}))
except ConflictError:
    pass
else:
    raise AssertionError("stale native definition was allowed to delete a changed row")
assert PeriodicTask.objects.filter(name="renamed").exists()
"""
    result = subprocess.run(
        [sys.executable, "-c", probe, str(tmp_path / "beat.sqlite3")],
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr
