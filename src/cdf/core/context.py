"""Context management utilities for managing the active workspace."""

import contextlib
import functools
import typing as t
from contextvars import ContextVar, Token

if t.TYPE_CHECKING:
    from cdf.core.injector import Lifecycle
    from cdf.core.workspace import Workspace

_ACTIVE_WORKSPACE: ContextVar[t.Optional["Workspace"]] = ContextVar(
    "active_workspace", default=None
)
"""The active workspace for resolving injected dependencies."""

_DEFAULT_LIFECYCLE: ContextVar[t.Optional["Lifecycle"]] = ContextVar(
    "default_lifecycle", default=None
)
"""The default lifecycle when wrapping functions with `Dependency.wrap`."""


def get_active_workspace() -> t.Optional["Workspace"]:
    """Get the active workspace for resolving injected dependencies."""
    return _ACTIVE_WORKSPACE.get()


def set_active_workspace(workspace: t.Optional["Workspace"]) -> Token:
    """Set the active workspace for resolving injected dependencies."""
    return _ACTIVE_WORKSPACE.set(workspace)


@contextlib.contextmanager
def use_workspace(workspace: t.Optional["Workspace"]) -> t.Iterator[None]:
    """Context manager for temporarily setting the active workspace."""
    token = set_active_workspace(workspace)
    try:
        yield
    finally:
        set_active_workspace(token.old_value)


T = t.TypeVar("T")


@t.overload
def resolve(
    dependencies: t.Callable[..., T],
    configuration: bool = ...,
) -> t.Callable[..., T]: ...


@t.overload
def resolve(
    dependencies: bool = ...,
    configuration: bool = ...,
) -> t.Callable[[t.Callable[..., T]], t.Callable[..., T]]: ...


def resolve(
    dependencies: t.Union[t.Callable[..., T], bool] = True,
    configuration: bool = True,
) -> t.Callable[..., t.Union[T, t.Callable[..., T]]]:
    """Decorator for injecting dependencies and resolving configuration for a function."""

    def resolve(func: t.Callable[..., T]) -> t.Callable[..., T]:
        @functools.wraps(func)
        def wrapper(*args: t.Any, **kwargs: t.Any) -> T:
            nonlocal func
            workspace = get_active_workspace()
            if workspace is None:
                return func(*args, **kwargs)
            if configuration:
                func = workspace.conf_resolver.resolve_defaults(func)
            if dependencies:
                func = workspace.container.wire(func)
            return func(*args, **kwargs)

        return wrapper

    if callable(dependencies):
        return resolve(dependencies)

    return resolve


def get_default_lifecycle() -> t.Optional["Lifecycle"]:
    """Get the default lifecycle when wrapping functions with `Dependency.wrap`."""
    return _DEFAULT_LIFECYCLE.get()


def set_default_lifecycle(lifecycle: t.Optional["Lifecycle"]) -> Token:
    """Set the default lifecycle when wrapping functions with `Dependency.wrap`."""
    return _DEFAULT_LIFECYCLE.set(lifecycle)


@contextlib.contextmanager
def use_default_lifecycle(lifecycle: t.Optional["Lifecycle"]) -> t.Iterator[None]:
    """Context manager for temporarily setting the default lifecycle."""
    token = set_default_lifecycle(lifecycle)
    try:
        yield
    finally:
        set_default_lifecycle(token.old_value)
