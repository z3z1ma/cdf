import pdb
import sys
import traceback
import typing as t

import cdf.core.constants as c
import cdf.core.context as context
from cdf.core.project import Project, get_project
from cdf.core.runtime.pipeline import pipeline_factory as pipeline

if t.TYPE_CHECKING:
    from sqlmesh.core.config import GatewayConfig


def execute() -> bool:
    """Check if the current module is being run as the main program in cdf context.

    Also injects a hook in debug mode to allow dropping into user code via pdb.
    """
    frame = sys._getframe(1)
    name = frame.f_globals["__name__"]

    _main = frame.f_globals.get(c.CDF_MAIN)
    proceed = name == "__main__" or name == _main

    if proceed and context.debug_mode.get():

        def debug_hook(etype, value, tb) -> None:
            traceback.print_exception(etype, value, tb)
            pdb.post_mortem(tb)

        sys.excepthook = debug_hook

    return proceed


def get_gateway(project: Project, workspace: str, sink: str) -> "GatewayConfig":
    """Get a sqlmesh gateway from a project sink."""
    return (
        project.get_workspace(workspace)
        .bind(lambda w: w.get_sink(sink))
        .map(lambda s: s.sink_transform())
        .unwrap()
    )


__all__ = ["pipeline", "execute", "get_project", "get_gateway"]
