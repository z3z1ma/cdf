import functools
import typing as t

import cdf.legacy.logger as logger
from cdf.legacy.project import Project, Workspace
from cdf.legacy.specification.base import BaseComponent
from cdf.types import P

T = t.TypeVar("T")


def _get_project(obj: t.Any) -> Project:
    """Get the project associated with the object."""
    if isinstance(obj, Project):
        return obj
    if isinstance(obj, Workspace):
        return obj.project
    if isinstance(obj, BaseComponent):
        return obj.workspace.project
    raise TypeError(f"Expected a Project, Workspace or Component, got {type(obj)}")


def with_activate_project(func: t.Callable[P, T]) -> t.Callable[P, T]:
    """Attempt to inject the Project associated with the first argument into cdf.context.

    Args:
        func: The function to decorate.

    Returns:
        The decorated function.
    """

    @functools.wraps(func)
    def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
        try:
            project = _get_project(args[0])
        except TypeError:
            logger.warning(f"Could not get project from {type(args[0])}")
            return func(*args, **kwargs)
        with project.activated():
            return func(*args, **kwargs)

    return wrapper
