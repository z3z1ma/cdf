"""Context module."""
import typing as t

if t.TYPE_CHECKING:
    from cdf.core.workspace import Workspace

_ACTIVE_WORKSPACE: "Workspace | None" = None
"""The active workspace."""


def set_active_workspace(workspace: "Workspace | None") -> None:
    """Set the active workspace."""
    global _ACTIVE_WORKSPACE
    _ACTIVE_WORKSPACE = workspace


def get_active_workspace() -> "Workspace | None":
    """Get the active workspace."""
    return _ACTIVE_WORKSPACE


_ANON_PROJECT_NUMBER: int = 1
"""A counter for anonymous project names."""


def get_project_number() -> int:
    """Get an anonymous project name, increments on access."""
    global _ANON_PROJECT_NUMBER

    ix = _ANON_PROJECT_NUMBER
    _ANON_PROJECT_NUMBER += 1
    return ix


_AUTOINSTALL_ENABLED: bool = False
"""A flag which indicates if autoinstall is enabled."""


def enable_autoinstall() -> None:
    """Enable autoinstall."""
    global _AUTOINSTALL_ENABLED
    _AUTOINSTALL_ENABLED = True


def disable_autoinstall() -> None:
    """Disable autoinstall."""
    global _AUTOINSTALL_ENABLED
    _AUTOINSTALL_ENABLED = False


def is_autoinstall_enabled() -> bool:
    """Check if autoinstall is enabled."""
    return _AUTOINSTALL_ENABLED
