"""The context module provides thread-safe context variables and injection mechanisms.

It facilitates communication between specifications and runtime modules.
"""

import typing as t
from contextvars import ContextVar

import dlt

if t.TYPE_CHECKING:
    from cdf.core.project import Workspace


active_workspace: ContextVar["Workspace"] = ContextVar("active_workspace")
"""The active workspace context variable.

The allows the active workspace to be passed to user-defined scripts. The workspace
has a reference to the project configuration and filesystem.
"""

active_pipeline: ContextVar[dlt.Pipeline] = ContextVar("active_pipeline")
"""Stores the active pipeline.

This is the primary mechanism to pass a configured pipeline to user-defined scripts.
"""

debug_mode: ContextVar[bool] = ContextVar("debug_mode", default=False)
"""The debug mode context variable.

Allows us to mutate certain behaviors in the runtime based on the debug mode. User can
optionally introspect this.
"""

extract_limit: ContextVar[int] = ContextVar("extract_limit", default=0)
"""The extract limit context variable.

Lets us set a limit on the number of items to extract from a source. This variable
can be introspected by user-defined scripts to optimize for partial extraction.
"""


__all__ = ["active_workspace", "active_pipeline", "debug_mode", "extract_limit"]
