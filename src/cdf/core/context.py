"""The context module provides thread-safe context variables and injection mechanisms.

It facilitates communication between specifications and runtime modules.
"""

import typing as t
from contextvars import ContextVar

import dlt

if t.TYPE_CHECKING:
    from cdf.core.project import Workspace


active_workspace: ContextVar["Workspace"] = ContextVar("active_workspace")
"""The active workspace context variable."""

active_pipeline: ContextVar[dlt.Pipeline] = ContextVar("active_pipeline")
"""Stores the active pipeline."""

debug_mode: ContextVar[bool] = ContextVar("debug_mode", default=False)
"""The debug mode context variable."""


__all__ = ["active_workspace", "active_pipeline", "debug_mode"]
