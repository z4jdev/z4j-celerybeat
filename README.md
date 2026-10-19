# z4j-celerybeat

[![PyPI version](https://img.shields.io/pypi/v/z4j-celerybeat.svg)](https://pypi.org/project/z4j-celerybeat/)
[![Python](https://img.shields.io/pypi/pyversions/z4j-celerybeat.svg)](https://pypi.org/project/z4j-celerybeat/)
[![License](https://img.shields.io/pypi/l/z4j-celerybeat.svg)](https://github.com/z4jdev/z4j-celerybeat/blob/main/LICENSE)

The Celery Beat scheduler adapter for [z4j](https://z4j.com).

Surfaces periodic / crontab / one-shot Celery schedules on the
dashboard's Schedules page. Database-backed `django_celery_beat.models.PeriodicTask`
supports native writes; Celery's static `app.conf.beat_schedule` is read-only.

Native writes and source confirmation need the z4j server, `z4j-core`,
`z4j-bare` and `z4j-celerybeat` from the same release, plus the Beat producer
setup described under Install.

## Compatibility

- Celery 5.3+ (no upper cap)
- django-celery-beat 2.5+ (for the writable backend)
- Python 3.11+

Full per-adapter matrix at <https://docs.z4j.com/reference/compatibility/>.

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
pip install z4j-django z4j-celery 'z4j-celerybeat[django]'
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

Start a long-lived `celery beat` process with
`--scheduler django_celery_beat.schedulers:DatabaseScheduler`, the same Django
settings and `Z4J_*` credentials. The Celery worker bootstrap alone registers
the engine, not a Beat scheduler. Run only one Beat instance for a given
schedule database unless you provide an upstream leader-election solution.

After the scheduler agent connects, its inventory snapshot includes existing
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

Native changes return an accepted operation first; the dashboard reports
completion after a full source snapshot confirms the result. A timeout or
uncertain result requires checking the native row before issuing a new request.
Clocked schedules are one-shot and require an offset-aware timestamp. Solar
schedules retain their coordinates; the common dashboard form does not create
solar definitions. See the guide for supported fields and source identities.

## Pairs with

- [`z4j-celery`](https://github.com/z4jdev/z4j-celery), engine adapter

## Reliability

- Direct application writes to `PeriodicTask` keep normal Django ORM
  semantics; reporting their post-save/post-delete signals to z4j is
  best-effort and does not roll back those writes.
- Dashboard controls execute in the agent process through the Django ORM. A
  database mutation that exceeds its 15-second timeout is reported as
  indeterminate because its worker thread may still commit it.

## Documentation

Full docs at [docs.z4j.com/schedulers/celery-beat/](https://docs.z4j.com/schedulers/celery-beat/).

## License

Apache-2.0, see [LICENSE](https://github.com/z4jdev/z4j-celerybeat/blob/main/LICENSE).

## Links

- Homepage: https://z4j.com
- Documentation: https://docs.z4j.com
- PyPI: https://pypi.org/project/z4j-celerybeat/
- Issues: https://github.com/z4jdev/z4j-celerybeat/issues
- Changelog: [CHANGELOG.md](https://github.com/z4jdev/z4j-celerybeat/blob/main/CHANGELOG.md)
- Security: security@z4j.com (see [SECURITY.md](https://github.com/z4jdev/z4j-celerybeat/blob/main/SECURITY.md))
