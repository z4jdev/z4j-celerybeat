# Changelog

All notable changes to `z4j-celerybeat` are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [1.1.0] - 2026-04-28

### Changed

- **v1.1.0 ecosystem family bump.** Pinned ``z4j-core>=1.1.0`` and ``z4j-bare>=1.1.0`` so a celery-beat scheduler adapter installed at 1.1.0 always resolves a known-good 1.1.0 slice of brain + agent. The brain-side z4j-scheduler 1.1.0 now takes over the "fire next tick" responsibility this adapter used to share with celery-beat's own loop; this adapter still owns the schedule CRUD path against the django-celery-beat tables. The matching dispatcher fix in z4j-bare 1.1.0 (route ``schedule.fire`` to the queue engine instead of the scheduler adapter) closes the loop so brain-side ticks reach the engine end-to-end.

## [1.0.1] - 2026-04-21

### Changed

- Lowered minimum Python version from 3.13 to 3.11. This package now supports Python 3.11, 3.12, 3.13, and 3.14.
- Documentation polish: standardized on ASCII hyphens across README, CHANGELOG, and docstrings for consistent rendering on PyPI.


## [1.0.0] - 2026-04

### Added

<!--
TODO: describe what ships in this first public release. One bullet per
capability. Examples:
- First public release.
- <Headline feature>
- <Second feature>
- N unit tests.
-->

- First public release.

## Links

- Repository: <https://github.com/z4jdev/z4j-celerybeat>
- Issues: <https://github.com/z4jdev/z4j-celerybeat/issues>
- PyPI: <https://pypi.org/project/z4j-celerybeat/>

[Unreleased]: https://github.com/z4jdev/z4j-celerybeat/compare/v1.0.0...HEAD
[1.0.0]: https://github.com/z4jdev/z4j-celerybeat/releases/tag/v1.0.0
