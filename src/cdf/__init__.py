import pdb
import sys
import traceback
import typing as t
from contextlib import suppress

import cdf.core.constants as c
import cdf.core.context as context
from cdf.core.project import Project, get_project
from cdf.core.runtime.pipeline import pipeline_factory as pipeline
from cdf.types import M, PathLike

if t.TYPE_CHECKING:
    from sqlmesh.core.config import GatewayConfig


@M.result
def find_nearest(path: PathLike) -> Project:
    """Find the nearest project.

    Recursively searches for a project file in the parent directories.
    """
    return get_project(path).unwrap()


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


def get_gateways(
    project: Project, workspace: str
) -> M.Result[t.Dict[str, "GatewayConfig"], Exception]:
    """Convert the project's gateways to a dictionary."""
    w = project.get_workspace(workspace).unwrap()
    gateways = {}
    for sink in w.sinks.values():
        with suppress(KeyError):
            gateways[sink.name] = sink.sink_transform()
    if not gateways:
        return M.error(ValueError(f"No gateways in workspace {workspace}"))
    return M.ok(gateways)


__all__ = ["pipeline", "execute", "get_project", "get_gateways"]
