# z4j-celerybeat

[![PyPI version](https://img.shields.io/pypi/v/z4j-celerybeat.svg)](https://pypi.org/project/z4j-celerybeat/)
[![Python](https://img.shields.io/pypi/pyversions/z4j-celerybeat.svg)](https://pypi.org/project/z4j-celerybeat/)
[![License](https://img.shields.io/pypi/l/z4j-celerybeat.svg)](https://github.com/z4jdev/z4j-celerybeat/blob/main/LICENSE)


**License:** Apache 2.0
**Status:** v1.0.0 - first public release.

The Celery-Beat scheduler adapter for [z4j](https://z4j.com). Surfaces
periodic / crontab / one-shot schedules on the dashboard's Schedules
page - read, create, update, enable, disable, trigger-now, delete.

Supports both of Celery's scheduling backends:
- `app.conf.beat_schedule` (static config in Python)
- `django_celery_beat.models.PeriodicTask` (database-backed, dynamic)

## Install

```bash
pip install z4j-celery z4j-celerybeat
```

## What it ships on day 1

| Capability | Status |
|---|---|
| List schedules | ✅ |
| Read individual schedule by id | ✅ |
| Create new schedule | ✅ |
| Update (interval, crontab, args, kwargs, enabled flag) | ✅ |
| Enable / disable | ✅ |
| Trigger-now (fire the task immediately outside the schedule) | ✅ |
| Delete | ✅ |
| Live sync for `django-celery-beat` | ✅ (via `post_save` signal on `PeriodicTask`) |

## Configure

### With `django-celery-beat` (Django projects)

```python
# settings.py
INSTALLED_APPS = [
    # ...
    "django_celery_beat",
    "z4j_django",
]
# The z4j-celerybeat adapter is auto-registered when django_celery_beat
# is on INSTALLED_APPS.
```

The Schedules page picks up every `PeriodicTask` row immediately. Edits
flow both ways - dashboard changes are written through to the database,
and changes written directly to the model surface via signals.

### With static `beat_schedule` (non-Django or plain Celery)

```python
# celery_app.py
from celery import Celery
from z4j_bare import install_agent
from z4j_celery import CeleryEngineAdapter
from z4j_celerybeat import CeleryBeatAdapter

app = Celery("myproject", broker="redis://localhost")
app.conf.beat_schedule = {
    "cleanup-every-5-minutes": {
        "task": "myapp.tasks.cleanup",
        "schedule": 300.0,
    },
}

install_agent(
    engines=[CeleryEngineAdapter(celery_app=app)],
    schedulers=[CeleryBeatAdapter(celery_app=app)],
    brain_url="https://z4j.internal",
    token="z4j_agent_...",
    project_id="my-project",
)
```

Static `beat_schedule` is read-only by design - you can view, enable,
disable, and trigger-now, but create / update / delete are rejected
because those changes would need to round-trip through a deploy. The
dashboard hides buttons it can't honor.

## Reliability

- No exception from the adapter propagates to Celery Beat or Django
  request handlers.
- `post_save` signal handlers for `PeriodicTask` are wrapped in a top-
  level try/except - even if the brain is unreachable, the database
  write is never affected.

## Documentation

- [Celery-beat scheduler guide](https://z4j.dev/schedulers/celery-beat/)
- [Schedules concepts](https://z4j.dev/concepts/architecture/)

## License

Apache 2.0 - see [LICENSE](LICENSE).

## Links

- Homepage: <https://z4j.com>
- Documentation: <https://z4j.dev>
- Issues: <https://github.com/z4jdev/z4j-celerybeat/issues>
- Changelog: [CHANGELOG.md](CHANGELOG.md)
- Security: `security@z4j.com` (see [SECURITY.md](SECURITY.md))
