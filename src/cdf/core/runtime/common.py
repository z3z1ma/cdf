import functools
import typing as t

import cdf.core.context as context
from cdf.core.specification.base import BaseComponent
from cdf.types import P

T = t.TypeVar("T")


def with_configured_spec(
    func: t.Callable[P, T],
) -> t.Callable[P, T]:
    """Decorator to inject the configured spec into a function."""

    @functools.wraps(func)
    def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
        spec = args[0]
        if not isinstance(spec, BaseComponent):
            raise TypeError(f"Expected a Component, got {type(spec)}")
        if not spec.has_workspace_association:
            return func(*args, **kwargs)
        token = context.active_workspace.set(spec.workspace)
        try:
            with spec.workspace.inject_configuration():
                return func(*args, **kwargs)
        finally:
            context.active_workspace.reset(token)

    return wrapper
