import typing as t
from enum import Enum, auto
from functools import wraps
from inspect import signature, unwrap

from typing_extensions import ParamSpec

T = t.TypeVar("T")
P = ParamSpec("P")


class Lifecycle(Enum):
    """Lifecycle of a dependency."""

    PROTOTYPE = auto()
    SINGLETON = auto()
    INSTANCE = auto()


class DependencyCycleError(Exception):
    """Raised when a dependency cycle is detected."""

    pass


class DependencyRegistry:
    def __init__(self) -> None:
        self._dependencies = {}
        self._singletons = {}
        self._wired = set()
        self._resolving = set()

    def add(
        self,
        name: str,
        dependency: t.Any,
        lifecycle=Lifecycle.INSTANCE,
        recursive: bool = True,
        **lazy_kwargs: t.Any,
    ) -> None:
        """Register a dependency with the container."""
        if lazy_kwargs and lifecycle is Lifecycle.INSTANCE:
            raise ValueError(
                "Cannot pass kwargs for instance dependencies. "
                "Please use prototype or singleton."
            )
        if recursive:
            dependency = self.inject_defaults(dependency)
        self._dependencies[name] = (dependency, lifecycle, lazy_kwargs)

    def get(self, name: str, must_exist: bool = False) -> t.Any:
        """Get a dependency"""
        if name not in self._dependencies:
            if must_exist:
                raise ValueError(f'Dependency "{name}" is not registered')
            return None

        if name in self._resolving:
            raise DependencyCycleError(f"Dependency cycle detected: {name}")

        dependency, lifecycle, maybe_kwargs = self._dependencies[name]
        if lifecycle == Lifecycle.PROTOTYPE:
            self._resolving.add(name)
            try:
                return (
                    dependency(**maybe_kwargs) if callable(dependency) else dependency
                )
            finally:
                self._resolving.remove(name)
        elif lifecycle == Lifecycle.SINGLETON:
            if name not in self._singletons:
                self._resolving.add(name)
                try:
                    self._singletons[name] = (
                        dependency(**maybe_kwargs)
                        if callable(dependency)
                        else dependency
                    )
                finally:
                    self._resolving.remove(name)
            return self._singletons[name]
        elif lifecycle == Lifecycle.INSTANCE:
            return dependency
        else:
            raise ValueError(f"Unknown lifecycle: {lifecycle}")

    def inject_defaults(self, func_or_cls: t.Callable[P, T]) -> t.Callable[P, T]:
        """Inject dependencies into a function."""
        _instance = unwrap(func_or_cls)
        if id(_instance) in self._wired or not callable(func_or_cls):
            return func_or_cls

        sig = signature(func_or_cls)

        @wraps(func_or_cls)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
            bound_args = sig.bind_partial(*args, **kwargs)
            for name, param in sig.parameters.items():
                if name not in bound_args.arguments:
                    dependency = self.get(name)
                    if dependency:
                        bound_args.arguments[name] = dependency
            return func_or_cls(*bound_args.args, **bound_args.kwargs)

        self._wired.add(id(_instance))
        return wrapper

    def wire(self, func_or_cls: t.Callable[P, T]) -> t.Callable[..., T]:
        """Wire dependencies into a callable recursively."""
        if not callable(func_or_cls):
            raise ValueError("Argument must be a callable")

        def recursive_inject(func: t.Callable[P, T]) -> t.Callable[P, T]:
            sig = signature(func)
            for name, param in sig.parameters.items():
                if name not in self._dependencies:
                    continue
                dependency, lifecycle, _ = self._dependencies[name]
                if callable(dependency):
                    dependency = recursive_inject(dependency)
                self._dependencies[name] = (dependency, lifecycle, _)
            return self.inject_defaults(func)

        return recursive_inject(func_or_cls)


GLOBAL_REGISTRY = DependencyRegistry()

__all__ = ["DependencyRegistry", "Lifecycle", "DependencyCycleError", "GLOBAL_REGISTRY"]
