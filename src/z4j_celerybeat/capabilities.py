"""Capability tokens advertised by the Celery-beat scheduler adapter."""

from __future__ import annotations

DEFAULT_CAPABILITIES: frozenset[str] = frozenset(
    {
        "list",
        "create",
        "update",
        "delete",
        "enable",
        "disable",
        "trigger_now",
    },
)


__all__ = ["DEFAULT_CAPABILITIES"]
