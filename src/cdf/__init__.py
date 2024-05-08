import pdb
import sys
import traceback
import typing as t
from pathlib import Path

import dlt
from dlt.sources.helpers import requests
from sqlmesh.core.config import ConnectionConfig, GatewayConfig, parse_connection_config

import cdf.core.constants as c
import cdf.core.context as context
import cdf.core.logger as logger
from cdf.core.project import Project, Workspace, load_project
from cdf.core.runtime import pipeline_factory as pipeline
from cdf.types import M, PathLike


@M.result
def find_nearest(path: PathLike = ".") -> Project:
    """Find the nearest project.

    Recursively searches for a project file in the parent directories.

    Args:
        path (PathLike, optional): The path to start searching from. Defaults to ".".

    Raises:
        FileNotFoundError: If no project is found.

    Returns:
        Project: The nearest project.
    """
    project = None
    path = Path(path).resolve()
    while path != path.parent:
        if p := load_project(path).unwrap_or(None):
            project = p
        path = path.parent
    if project is None:
        raise FileNotFoundError("No project found.")
    return project


def is_main(module_name: t.Optional[str] = None) -> bool:
    """Check if the current module is being run as the main program in cdf context.

    Also injects a hook in debug mode to allow dropping into user code via pdb.

    Args:
        module_name (str, optional): The name of the module to check. If None, the calling module is
            checked. The most idiomatic usage is to pass `__name__` to check the current module.

    Returns:
        bool: True if the current module is the main program in cdf context.
    """
    proceed = False
    frame = sys._getframe()
    while frame.f_back:
        program_name = frame.f_globals.get(c.CDF_MAIN)
        caller_mod = module_name or frame.f_globals["__name__"]
        if proceed := (module_name == "__main__" or caller_mod == program_name):
            break
        frame = frame.f_back

    if proceed and context.debug_mode.get():

        def debug_hook(etype, value, tb) -> None:
            traceback.print_exception(etype, value, tb)
            pdb.post_mortem(tb)

        sys.excepthook = debug_hook

    return proceed


def get_active_project() -> Project:
    """Get the active project.

    Raises:
        ValueError: If no valid project is found in the context.

    Returns:
        Project: The active project.
    """
    obj = context.active_project.get()
    if isinstance(obj, Project):
        return obj
    if isinstance(obj, Workspace):
        return obj.parent
    raise ValueError("No valid project found in context.")


def get_workspace(path: PathLike = ".") -> M.Result[Workspace, Exception]:
    """Get a workspace from a path.

    Args:
        path (PathLike, optional): The path to get the workspace from. Defaults to ".".

    Returns:
        M.Result[Workspace, Exception]: The workspace or an error.
    """
    return find_nearest(path).bind(lambda p: p.get_workspace_from_path(path))


with_config = dlt.sources.config.with_config

inject_config = dlt.config.value
inject_secret = dlt.secrets.value

session = requests.Client

transform_gateway = GatewayConfig
"""Gateway configuration for transforms."""


def transform_connection(type_: str, /, **kwargs) -> ConnectionConfig:
    """Create a connection configuration for transforms."""
    return parse_connection_config({"type": type_, **kwargs})


# TODO: lets add a param which allows us to pass a tuple of sources
# and maybe some lifecycle hooks
def run_script(
    module_name: str,
    /,
    source: t.Union[t.Callable[..., dlt.sources.DltSource], dlt.sources.DltSource],
    *,
    run_options: t.Optional[t.Dict[str, t.Any]] = None,
    **kwargs: t.Any,
) -> None:
    """A shorthand syntax for a cdf script with a single source which should run as a pipeline.

    The first argument should almost always be `__name__`. This function conditionally executes
    the source if the module is the main program in cdf context. This occurs either when invoked
    through cdf <workspace> pipeline command or when the script is run directly by python.
    """
    if is_main(module_name):
        if callable(source):
            source = source(**kwargs)
        run_options = run_options or {}
        print(pipeline().run(source, **run_options))


__all__ = [
    "pipeline",
    "is_main",
    "load_project",
    "find_nearest",
    "get_active_project",
    "get_workspace",
    "logger",
    "with_config",
    "inject_config",
    "inject_secret",
    "requests",
    "session",
]
