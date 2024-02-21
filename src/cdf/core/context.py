"""Context module."""
import typing as t
from contextvars import ContextVar
from types import SimpleNamespace

if t.TYPE_CHECKING:
    from cdf.core.parser import ParsedComponent
    from cdf.core.workspace import Workspace


active_workspace: ContextVar["Workspace"] = ContextVar("active_workspace")
debug: ContextVar[bool] = ContextVar("debug", default=False)
current_spec: ContextVar[SimpleNamespace] = ContextVar("current_pipeline")


def set_current_spec(component: "ParsedComponent") -> "ParsedComponent":
    """Set the current component specification."""
    ns = SimpleNamespace(
        **{k: v.unwrap_or(None) for k, v in component.specification.items()}
    )
    ns.name = component.name
    if not hasattr(ns, "version"):
        ns.version = 0
    ns.versioned_name = f"{component.name}_v{ns.version}"
    current_spec.set(ns)
    return component
