# z4j-celerybeat

[![PyPI version](https://img.shields.io/pypi/v/z4j-celerybeat.svg)](https://pypi.org/project/z4j-celerybeat/)
[![Python](https://img.shields.io/pypi/pyversions/z4j-celerybeat.svg)](https://pypi.org/project/z4j-celerybeat/)
[![License](https://img.shields.io/pypi/l/z4j-celerybeat.svg)](https://github.com/z4jdev/z4j-celerybeat/blob/main/LICENSE)

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
| Enable / disable | via the `PeriodicTask.enabled` field |
| Trigger now | fires the underlying task immediately, outside the schedule |
| Delete | django-celery-beat backend |
| Live sync | in-process django-celery-beat saves/deletes are reported best-effort through Django signals; periodic snapshots reconcile the source |
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

After the agent connects, its inventory snapshot includes existing
`PeriodicTask` rows. Dashboard changes write through to the database. Saves
and deletes in a process where the adapter's hooks are connected are reported
best-effort through Django signals; later inventory snapshots reconcile the
database source as well.

### With static `beat_schedule` (plain Celery)

```python
import os

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
    hmac_secret=os.environ["Z4J_HMAC_SECRET"],
)
```

## Pairs with

- [`z4j-celery`](https://github.com/z4jdev/z4j-celery), engine adapter

## Reliability

- Direct application writes to `PeriodicTask` keep normal Django ORM
  semantics; reporting their post-save/post-delete signals to z4j is
  best-effort and does not roll back those writes.
- Dashboard controls execute in the agent process through the Django ORM. A
  mutation that exceeds its 10-second timeout is reported as indeterminate
  because its worker thread may still commit it.

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
