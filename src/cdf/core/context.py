"""Context module."""
import typing as t
from contextvars import ContextVar

if t.TYPE_CHECKING:
    from cdf.core.workspace import Workspace


active_workspace: ContextVar["Workspace"] = ContextVar("active_workspace")
debug: ContextVar[bool] = ContextVar("debug", default=False)
