import functools
import typing as t

import cdf.core.context as context
import cdf.core.logger as logger
from cdf.core.project import Project, Workspace
from cdf.core.specification.base import BaseComponent
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


def with_configuration(func: t.Callable[P, T]) -> t.Callable[P, T]:
    """Attempt to inject the Project config associated with the first argument into dlt context.

    Further calls to dlt.config and dlt.secrets will reflect the configuration of the project. This
    function also sets the active project context variable.

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
        token = context.active_project.set(project)
        try:
            with project.inject_configuration():
                return func(*args, **kwargs)
        finally:
            context.active_project.reset(token)

    return wrapper


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
        token = context.active_project.set(project)
        try:
            return func(*args, **kwargs)
        finally:
            context.active_project.reset(token)

    return wrapper
