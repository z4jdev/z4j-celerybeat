# z4j-celerybeat

[![PyPI version](https://img.shields.io/pypi/v/z4j-celerybeat.svg?v=1.7.0)](https://pypi.org/project/z4j-celerybeat/)
[![Python](https://img.shields.io/pypi/pyversions/z4j-celerybeat.svg?v=1.7.0)](https://pypi.org/project/z4j-celerybeat/)
[![License](https://img.shields.io/pypi/l/z4j-celerybeat.svg?v=1.7.0)](https://github.com/z4jdev/z4j-celerybeat/blob/main/LICENSE)

The Celery Beat scheduler adapter for [z4j](https://z4j.com).

Surfaces periodic / crontab / one-shot Celery schedules on the
dashboard's Schedules page, read, create, update, enable, disable,
trigger, delete. Supports both Celery's static `app.conf.beat_schedule`
and the database-backed `django_celery_beat.models.PeriodicTask`.

## Compatibility

- Celery 5.3+ (no upper cap)
- django-celery-beat 2.5+ (for the writable backend)
- Python 3.11+

Full per-adapter matrix at <https://z4j.dev/reference/compatibility/>.

## What it ships

| Capability | Notes |
|---|---|
| List schedules | from both static config and django-celery-beat |
| Read individual schedule | by id |
| Create schedule | django-celery-beat backend (static is read-only) |
| Update | interval / crontab / args / kwargs / enabled flag |
| Enable / disable | via `is_enabled` toggle |
| Trigger now | fires the underlying task immediately, outside the schedule |
| Delete | django-celery-beat backend |
| Live sync | django-celery-beat changes flow to the dashboard automatically |
| Boot inventory | full snapshot at agent connect; existing schedules show up without editing |

Static `beat_schedule` is read-only by design, you can view and
trigger, but create / update / delete / enable / disable all need
django-celery-beat (or a source-code edit and deploy round-trip). The
dashboard hides buttons it can't honor.

## Install

```bash
pip install z4j-celery z4j-celerybeat
```

### With django-celery-beat (most Django projects)

```python
# settings.py
INSTALLED_APPS = [
    # ...
    "django_celery_beat",
    "z4j_django",
]
```

The Schedules page picks up every `PeriodicTask` row immediately. Edits
flow both ways, dashboard changes write through to the database, and
changes written directly to the model surface via Django signals.

### With static `beat_schedule` (plain Celery)

```python
from celery import Celery
from z4j_bare import install_agent
from z4j_celery import CeleryEngineAdapter
from z4j_celerybeat import CeleryBeatSchedulerAdapter

app = Celery("myproject", broker="redis://localhost")
app.conf.beat_schedule = {
    "cleanup-every-5-minutes": {
        "task": "myapp.tasks.cleanup",
        "schedule": 300.0,
    },
}

install_agent(
    engines=[CeleryEngineAdapter(celery_app=app)],
    schedulers=[CeleryBeatSchedulerAdapter(celery_app=app)],
    brain_url="https://brain.example.com",
    token="z4j_agent_...",
    project_id="my-project",
)
```

## Pairs with

- [`z4j-celery`](https://github.com/z4jdev/z4j-celery), engine adapter

## Reliability

- No exception from the adapter ever propagates back to Celery Beat,
  Django request handlers, or `PeriodicTask` signal receivers.
- Database writes for `PeriodicTask` happen in the dashboard's request
  context with normal Django ORM semantics, even if z4j is
  unreachable, the local model write is never affected.

## Documentation

Full docs at [z4j.dev/schedulers/celery-beat/](https://z4j.dev/schedulers/celery-beat/).

## License

Apache-2.0, see [LICENSE](LICENSE).

## Links

- Homepage: https://z4j.com
- Documentation: https://z4j.dev
- PyPI: https://pypi.org/project/z4j-celerybeat/
- Issues: https://github.com/z4jdev/z4j-celerybeat/issues
- Changelog: [CHANGELOG.md](CHANGELOG.md)
- Security: security@z4j.com (see [SECURITY.md](SECURITY.md))
