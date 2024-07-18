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
    lc = Lifecycle

    def __init__(self) -> None:
        """Initialize the registry."""
        self._singletons = {}
        self._dependencies = {}
        self._resolving = set()

    def add(
        self,
        name: str,
        dependency: t.Any,
        lifecycle=Lifecycle.INSTANCE,
        **lazy_kwargs: t.Any,
    ) -> None:
        """Register a dependency with the container."""
        if lazy_kwargs and lifecycle is Lifecycle.INSTANCE:
            raise ValueError(
                "Cannot pass kwargs for instance dependencies. "
                "Please use prototype or singleton."
            )
        if name in self._dependencies:
            raise ValueError(f'Dependency "{name}" is already registered')
        self._dependencies[name] = (dependency, lifecycle, lazy_kwargs)

    def remove(self, name: str) -> None:
        """Remove a dependency from the container."""
        if name in self._dependencies:
            del self._dependencies[name]
        if name in self._singletons:
            del self._singletons[name]

    def clear(self) -> None:
        """Clear all dependencies and singletons."""
        self._dependencies.clear()
        self._singletons.clear()

    def has(self, name: str) -> bool:
        """Check if a dependency is registered."""
        return name in self._dependencies

    def get(self, name: str, must_exist: bool = False) -> t.Any:
        """Get a dependency"""
        if name not in self._dependencies:
            if must_exist:
                raise KeyError(f'Dependency "{name}" is not registered')
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

    def __contains__(self, name: str) -> bool:
        """Check if a dependency is registered."""
        return self.has(name)

    def __getitem__(self, name: str) -> t.Any:
        """Get a dependency. Raises KeyError if not found."""
        return self.get(name, must_exist=True)

    def __setitem__(self, name: str, dependency: t.Any) -> None:
        """Add a dependency. Defaults to singleton lifecycle if callable, else instance."""
        self.add(
            name,
            dependency,
            Lifecycle.SINGLETON if callable(dependency) else Lifecycle.INSTANCE,
        )

    def __delitem__(self, name: str) -> None:
        """Remove a dependency."""
        self.remove(name)

    def inject_defaults(self, func_or_cls: t.Callable[P, T]) -> t.Callable[P, T]:
        """Inject dependencies into a function."""
        _instance = unwrap(func_or_cls)
        if not callable(func_or_cls):
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

    def __call__(
        self, func_or_cls: t.Callable[P, T], *args: t.Any, **kwargs: t.Any
    ) -> T:
        """Invoke a callable with dependencies injected from the registry."""
        return self.wire(func_or_cls)(*args, **kwargs)

    def __iter__(self) -> t.Iterator[str]:
        """Iterate over dependency names."""
        return iter(self._dependencies)

    def __len__(self) -> int:
        """Return the number of dependencies."""
        return len(self._dependencies)

    def __repr__(self) -> str:
        return f"<DependencyRegistry {self._dependencies.keys()}>"

    def __str__(self) -> str:
        return repr(self)


GLOBAL_REGISTRY = DependencyRegistry()

__all__ = ["DependencyRegistry", "Lifecycle", "DependencyCycleError", "GLOBAL_REGISTRY"]
