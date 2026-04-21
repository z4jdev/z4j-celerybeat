"""Schedule storage sources for the celery-beat scheduler adapter.

Two sources, both optional and tried in order:

- :class:`DjangoCeleryBeatSource` - reads/writes
  ``django_celery_beat.models.PeriodicTask``. Requires
  ``django-celery-beat`` installed and Django apps ready.
- :class:`StaticBeatScheduleSource` - reads (read-only) entries
  from ``celery_app.conf.beat_schedule``. Suitable for projects
  that hard-code their beat schedule in ``celery.py``.

The scheduler adapter holds an ordered tuple of sources and routes
each operation to the first one that supports it.
"""

from __future__ import annotations

from z4j_celerybeat.sources.dcb import DjangoCeleryBeatSource
from z4j_celerybeat.sources.static import StaticBeatScheduleSource

__all__ = [
    "DjangoCeleryBeatSource",
    "StaticBeatScheduleSource",
]
