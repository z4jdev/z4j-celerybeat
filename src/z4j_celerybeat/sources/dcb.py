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

import json
import logging
from decimal import Decimal
from typing import Any

from z4j_core.errors import ConflictError, NotFoundError
from z4j_core.models import (
    CommandResult,
    Schedule,
    ScheduleKind,
    refuse_unimplemented_overlap,
)
from z4j_core.schedule_external import (
    external_schedule_configuration,
    normalize_external_schedule,
)

from z4j_celerybeat._offload import (
    OffloadTimeoutError,
    indeterminate_timeout_result,
    offload,
)
from z4j_celerybeat.mapper import map_periodic_task

#: M4: DCB Django-ORM I/O runs on the dedicated bounded offload pool (not
#: the shared default executor via asyncio.to_thread) under a timeout, so a
#: slow DB cannot starve the agent's heartbeat/reconnect threads. On timeout
#: offload raises OffloadTimeoutError (the op did not complete) rather than
#: hanging the loop forever.
_DCB_OFFLOAD_TIMEOUT = 15.0

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
        if self._models is not None:
            return True
        try:
            from django_celery_beat import models  # type: ignore[import-not-found]
        except (ImportError, Exception):
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
        return await offload(self._list_schedules_sync, timeout=_DCB_OFFLOAD_TIMEOUT)

    def _list_schedules_sync(self) -> list[Schedule]:
        models = self._models
        rows = list(
            models.PeriodicTask.objects.select_related(
                "interval",
                "crontab",
                "clocked",
                "solar",
            ).all(),
        )
        return [map_periodic_task(row) for row in rows]

    async def get_schedule(self, schedule_id: str) -> Schedule | None:
        if not self.is_available():
            return None
        return await offload(self._get_schedule_sync, schedule_id, timeout=_DCB_OFFLOAD_TIMEOUT)

    def _get_schedule_sync(self, schedule_id: str) -> Schedule | None:
        models = self._models
        qs = models.PeriodicTask.objects.select_related(
            "interval",
            "crontab",
            "clocked",
            "solar",
        )
        if schedule_id.startswith("static:"):
            return None
        if schedule_id.startswith("dcb:"):
            try:
                return map_periodic_task(qs.get(pk=int(schedule_id[4:])))
            except (models.PeriodicTask.DoesNotExist, ValueError):
                return None
        # Legacy callers used names or unqualified IDs. New projections use
        # dcb:<pk>, so a numeric task name can never shadow a command's ID.
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
        try:
            return await offload(self._create_schedule_sync, spec, timeout=_DCB_OFFLOAD_TIMEOUT)
        except OffloadTimeoutError:
            # RM4: create_schedule must return a Schedule, so it cannot carry an
            # indeterminate CommandResult; a timeout propagates as the explicit
            # "did-not-complete" signal (the reconciler retries next cycle, and
            # the create is guarded by an exists() check so a re-run is a no-op).
            # Log it so a write that later commits is not silently a bare timeout.
            logger.warning(
                "z4j celerybeat: create_schedule %r timed out after %ss; the row "
                "may still have been committed and will reconcile on the next sync",
                spec.name,
                _DCB_OFFLOAD_TIMEOUT,
            )
            raise

    def _create_schedule_sync(self, spec: Schedule) -> Schedule:
        # THE enforceable refusal, as opposed to the model's.
        #
        # This source persists a Schedule's attributes and never serializes it,
        # and django-celery-beat has no column for an overlap policy, so a spec
        # carrying ``skip`` was written without it and mapped back as ``allow``:
        # the caller asked for collision prevention, was told it was applied,
        # and got concurrent runs.
        #
        # z4j-core refuses the same value, but every guard available there sits
        # on a hook a subclass can override -- validator, serializer, copy,
        # model_construct, subclass default, model_post_init -- and a caller
        # who overrides one reaches this function with ``skip`` intact. That is
        # not a hole to be plugged with an eighth hook; it is what a Python
        # class is. The boundary that can actually hold is the one the caller
        # does not own, which is here.
        refuse_unimplemented_overlap(getattr(spec, "overlap_policy", None))

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
            "one_off": spec.kind == ScheduleKind.CLOCKED,
        }
        kwargs.update(related)
        row = models.PeriodicTask.objects.create(**kwargs)
        return map_periodic_task(row)

    async def update_schedule(self, schedule_id: str, spec: Schedule) -> Schedule:
        if not self.is_available():
            raise NotFoundError(
                "django-celery-beat is not installed; cannot update schedules",
            )
        try:
            return await offload(
                self._update_schedule_sync, schedule_id, spec, timeout=_DCB_OFFLOAD_TIMEOUT
            )
        except OffloadTimeoutError:
            # RM4: update_schedule must return a Schedule (no indeterminate
            # CommandResult channel); a timeout propagates as the explicit
            # "did-not-complete" signal. The update is idempotent (it re-applies
            # the same spec), so the reconciler safely retries next cycle.
            logger.warning(
                "z4j celerybeat: update_schedule %r timed out after %ss; the row "
                "may still have been committed and will reconcile on the next sync",
                schedule_id,
                _DCB_OFFLOAD_TIMEOUT,
            )
            raise

    def _resolve_periodic_task(self, schedule_id: str) -> object:
        """Look up a PeriodicTask by name or pk."""
        models = self._models
        if schedule_id.startswith("static:"):
            raise NotFoundError("static Beat schedules are read-only")
        if schedule_id.startswith("dcb:"):
            try:
                return models.PeriodicTask.objects.get(pk=int(schedule_id[4:]))
            except (models.PeriodicTask.DoesNotExist, ValueError) as exc:
                raise NotFoundError(f"schedule {schedule_id!r} not found") from exc
        try:
            return models.PeriodicTask.objects.get(name=schedule_id)
        except models.PeriodicTask.DoesNotExist:
            pass
        try:
            return models.PeriodicTask.objects.get(pk=int(schedule_id))
        except (models.PeriodicTask.DoesNotExist, ValueError) as exc:
            raise NotFoundError(f"schedule {schedule_id!r} not found") from exc

    def _update_schedule_sync(self, schedule_id: str, spec: Schedule) -> Schedule:
        # THE enforceable refusal, as opposed to the model's.
        #
        # This source persists a Schedule's attributes and never serializes it,
        # and django-celery-beat has no column for an overlap policy, so a spec
        # carrying ``skip`` was written without it and mapped back as ``allow``:
        # the caller asked for collision prevention, was told it was applied,
        # and got concurrent runs.
        #
        # z4j-core refuses the same value, but every guard available there sits
        # on a hook a subclass can override -- validator, serializer, copy,
        # model_construct, subclass default, model_post_init -- and a caller
        # who overrides one reaches this function with ``skip`` intact. That is
        # not a hole to be plugged with an eighth hook; it is what a Python
        # class is. The boundary that can actually hold is the one the caller
        # does not own, which is here.
        refuse_unimplemented_overlap(getattr(spec, "overlap_policy", None))

        row = self._resolve_periodic_task(schedule_id)

        row.name = spec.name

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
        row.one_off = spec.kind == ScheduleKind.CLOCKED
        row.save()
        return map_periodic_task(row)

    async def delete_schedule(self, schedule_id: str) -> CommandResult:
        if not self.is_available():
            return CommandResult(status="success")
        try:
            return await offload(
                self._delete_schedule_sync, schedule_id, timeout=_DCB_OFFLOAD_TIMEOUT
            )
        except OffloadTimeoutError:
            # RM4: a DELETE that exceeds the timeout may still commit on the DB.
            # Reporting a clean "failed" would invite the operator to repeat a
            # mutation whose outcome is unknown; surface it as indeterminate.
            return indeterminate_timeout_result(
                "delete_schedule",
                _DCB_OFFLOAD_TIMEOUT,
                hint="the schedule may still have been deleted",
            )

    def _delete_schedule_sync(self, schedule_id: str) -> CommandResult:
        try:
            row = self._resolve_periodic_task(schedule_id)
        except NotFoundError:
            if schedule_id.startswith("dcb:"):
                return CommandResult(status="success", result={"deleted": 0})
            return CommandResult(status="failed", error=f"schedule {schedule_id!r} not found")
        row.delete()
        return CommandResult(
            status="success",
            result={"deleted": 1},
        )

    async def set_enabled(self, schedule_id: str, enabled: bool) -> CommandResult:
        if not self.is_available():
            return CommandResult(
                status="failed",
                error="django-celery-beat is not installed",
            )
        try:
            return await offload(
                self._set_enabled_sync, schedule_id, enabled, timeout=_DCB_OFFLOAD_TIMEOUT
            )
        except OffloadTimeoutError:
            # RM4: an enabled-toggle that exceeds the timeout may still commit;
            # report indeterminate rather than a definitive failure the operator
            # would blindly retry.
            return indeterminate_timeout_result(
                "set_enabled",
                _DCB_OFFLOAD_TIMEOUT,
                hint=f"the enabled={enabled} toggle may still have landed",
            )

    def _set_enabled_sync(self, schedule_id: str, enabled: bool) -> CommandResult:
        from django.db import transaction

        models = self._models
        with transaction.atomic():
            try:
                found = self._resolve_periodic_task(schedule_id)
            except NotFoundError:
                return CommandResult(status="failed", error=f"schedule {schedule_id!r} not found")
            row = models.PeriodicTask.objects.select_for_update().get(pk=found.pk)
            row.enabled = enabled
            # Native save resets last_run_at when disabling and advances
            # PeriodicTasks.last_update, which tells a running Beat to reload.
            row.save(update_fields=["enabled", "last_run_at", "date_changed"])
        return CommandResult(
            status="success",
            result={"schedule_id": schedule_id, "enabled": enabled},
        )

    # ------------------------------------------------------------------
    # Schedule-object construction
    # ------------------------------------------------------------------

    async def apply_external_mutation(
        self,
        *,
        operation: str,
        schedule_id: str | None,
        expected: dict[str, Any] | None,
        definition: dict[str, Any],
    ) -> Schedule | None:
        if not self.is_available():
            raise NotFoundError("django-celery-beat is not available")
        return await offload(
            self._mutate_sync,
            operation,
            schedule_id,
            expected,
            definition,
            timeout=_DCB_OFFLOAD_TIMEOUT,
        )

    def _mutate_sync(
        self,
        operation: str,
        schedule_id: str | None,
        expected: dict[str, Any] | None,
        definition: dict[str, Any],
    ) -> Schedule | None:
        from datetime import UTC, datetime
        from uuid import uuid4

        from django.db import transaction

        if operation not in {"create", "update", "delete"}:
            raise ValueError("unsupported native schedule operation")
        allowed = {
            "name",
            "task_name",
            "kind",
            "expression",
            "timezone",
            "queue",
            "args",
            "kwargs",
            "is_enabled",
        }
        if set(definition) - allowed:
            raise ValueError("unsupported native schedule fields")
        with transaction.atomic():
            if operation == "create":
                if expected is not None or schedule_id is not None:
                    raise ValueError("native creation cannot bind an existing schedule")
                now = datetime.now(UTC)
                spec_data = dict(
                    definition,
                    id=str(uuid4()),
                    project_id=str(uuid4()),
                    engine="celery",
                    scheduler="celery-beat",
                    created_at=now.isoformat(),
                    updated_at=now.isoformat(),
                )
                spec = Schedule.model_validate_json(json.dumps(spec_data))
                return self._create_schedule_sync(spec)
            if not schedule_id or not schedule_id.startswith("dcb:") or expected is None:
                raise ValueError("native writes require an exact database schedule identity")
            try:
                row = self._models.PeriodicTask.objects.select_for_update().get(
                    pk=int(schedule_id[4:])
                )
            except self._models.PeriodicTask.DoesNotExist as exc:
                raise NotFoundError("native schedule no longer exists") from exc
            prior = map_periodic_task(row)
            observed = normalize_external_schedule(
                prior.model_dump(mode="json"), owner="celery-beat"
            )
            if external_schedule_configuration(observed) != external_schedule_configuration(
                expected
            ):
                raise ConflictError("native schedule changed; refresh before editing")
            if operation == "delete":
                row.delete()
                return None
            data = prior.model_dump(mode="json")
            data.update(definition)
            spec = Schedule.model_validate_json(json.dumps(data))
            return self._update_schedule_sync(schedule_id, spec)

    def _build_related_schedule_object(
        self,
        spec: Schedule,
    ) -> dict[str, Any]:
        """Create or fetch the IntervalSchedule / CrontabSchedule for ``spec``."""
        models = self._models
        if spec.kind == ScheduleKind.INTERVAL:
            seconds = Decimal(spec.expression or "0")
            if not seconds.is_finite() or seconds <= 0:
                raise ValueError("interval must be a positive number of seconds")
            micros = seconds * 1_000_000
            if micros != micros.to_integral_value():
                raise ValueError("interval cannot be more precise than microseconds")
            integral = seconds == seconds.to_integral_value()
            interval, _ = models.IntervalSchedule.objects.get_or_create(
                every=int(seconds if integral else micros),
                period=models.IntervalSchedule.SECONDS
                if integral
                else models.IntervalSchedule.MICROSECONDS,
            )
            return {"interval": interval}

        if spec.kind == ScheduleKind.CRON:
            parts = (spec.expression or "* * * * *").split()
            if len(parts) != 5:
                raise ValueError("cron expression must contain exactly five fields")
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
            if clocked_time.tzinfo is None:
                raise ValueError("clocked expression must include a timezone offset")
            clocked, _ = models.ClockedSchedule.objects.get_or_create(
                clocked_time=clocked_time,
            )
            return {"clocked": clocked}

        if spec.kind == ScheduleKind.SOLAR:
            coordinates = spec.metadata.get("solar")
            if (
                not isinstance(coordinates, dict)
                or not {"latitude", "longitude"} <= coordinates.keys()
            ):
                raise ValueError("solar schedules require explicit latitude and longitude")
            solar, _ = models.SolarSchedule.objects.get_or_create(
                event=spec.expression,
                latitude=coordinates["latitude"],
                longitude=coordinates["longitude"],
            )
            return {"solar": solar}

        raise NotFoundError(f"unsupported schedule kind {spec.kind!r}")


__all__ = ["DjangoCeleryBeatSource"]
