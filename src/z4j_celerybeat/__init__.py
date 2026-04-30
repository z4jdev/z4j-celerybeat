"""z4j-celerybeat - Celery-beat scheduler adapter for z4j.

Public API:

- :class:`CeleryBeatSchedulerAdapter` - pass this to the agent
  runtime alongside the :class:`z4j_celery.CeleryEngineAdapter`.
- :class:`DjangoCeleryBeatSource` - exposed for advanced users
  who want to construct a custom source list.
- :class:`StaticBeatScheduleSource` - same.

Licensed under Apache License 2.0.
"""

from __future__ import annotations

from z4j_celerybeat.scheduler import CeleryBeatSchedulerAdapter
from z4j_celerybeat.sources import DjangoCeleryBeatSource, StaticBeatScheduleSource

__version__ = "1.3.0"

__all__ = [
    "CeleryBeatSchedulerAdapter",
    "DjangoCeleryBeatSource",
    "StaticBeatScheduleSource",
    "__version__",
]
