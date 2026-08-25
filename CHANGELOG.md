# Changelog

## 1.9.0 (2026-08-25)

* No functional change. Version bumped as part of the coordinated 1.9.0 fleet release, so every package in a deployment agrees on its peers.

## 1.8.0 (2026-07-23)

* `trigger_now` offloads its broker I/O off the agent loop so a broker incident can no longer freeze it; a timed-out mutation is reported indeterminate.
* Part of the coordinated 1.8.0 fleet release (unified fleet version, green lint/format/import-boundary gate).

## 1.7.0 (2026-07-07)

* README corrected to the real `CeleryBeatSchedulerAdapter` API.
* Python 3.11 is now the minimum supported version (3.10 dropped).
* Part of the coordinated 1.7.0 fleet release (unified fleet version, green lint/format/import-boundary gate).

## 1.4.0 (2026-05-02)

Initial 1.4.0 release: celery-beat scheduler companion. Surfaces existing celery-beat schedules in the dashboard.
