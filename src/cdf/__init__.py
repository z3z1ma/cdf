import pdb
import sys
import traceback
import typing as t
from contextlib import suppress
from pathlib import Path

import cdf.core.constants as c
import cdf.core.context as context
from cdf.core.project import Project, Workspace, load_project
from cdf.core.runtime.pipeline import pipeline_factory as pipeline
from cdf.types import M, PathLike

if t.TYPE_CHECKING:
    from sqlmesh.core.config import GatewayConfig


@M.result
def find_nearest(path: PathLike) -> Project:
    """Find the nearest project.

    Recursively searches for a project file in the parent directories.
    """
    project = None
    path = Path(path)
    while path != path.parent:
        if p := load_project(path).unwrap_or(None):
            project = p
        path = path.parent
    if project is None:
        raise FileNotFoundError("No project found.")
    return project


def execute() -> bool:
    """Check if the current module is being run as the main program in cdf context.

    Also injects a hook in debug mode to allow dropping into user code via pdb.
    """
    frame = sys._getframe(1)

    _main = frame.f_globals.get(c.CDF_MAIN)
    proceed = frame.f_globals["__name__"] in ("__main__", _main)

    if proceed and context.debug_mode.get():

        def debug_hook(etype, value, tb) -> None:
            traceback.print_exception(etype, value, tb)
            pdb.post_mortem(tb)

        sys.excepthook = debug_hook

    return proceed


def get_active_project() -> Project:
    """Get the active project."""
    obj = context.active_project.get()
    if isinstance(obj, Project):
        return obj
    if isinstance(obj, Workspace):
        return obj.parent
    raise ValueError("No valid project found in context.")


def get_workspace_from_path(path: PathLike) -> M.Result[Workspace, Exception]:
    """Get a workspace from a path."""
    return find_nearest(path).bind(lambda p: p.get_workspace_from_path(path))


__all__ = [
    "pipeline",
    "execute",
    "load_project",
    "find_nearest",
    "get_active_project",
    "get_workspace_from_path",
]
