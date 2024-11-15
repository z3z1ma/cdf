"""Context module for dependency injection."""

import asyncio
import atexit
import contextlib
import inspect
import sys
import threading
import typing as t
from contextvars import ContextVar
from functools import wraps
from types import MappingProxyType

if sys.version_info >= (3, 9):
    from typing import ParamSpec
else:
    from typing_extensions import ParamSpec

from cdf.core.configuration import ConfigBox
from cdf.core.constants import CONTEXT_PARAM_NAME

T = t.TypeVar("T")
P = ParamSpec("P")


__all__ = [
    "Container",
    "injected",
    "register_dep",
    "active_container",
]

injected = object()
"""Sentinel value to indicate that a dependency MUST be injected."""


class DependencyCycleError(RuntimeError):
    """Raised when a dependency cycle is detected."""


class DependencyNotFoundError(KeyError):
    """Raised when a dependency is not found in the context."""


class Container(t.MutableMapping[str, t.Any]):
    """Provides access to configuration and acts as a DI container with dependency resolution."""

    active: t.ClassVar[ContextVar["Container"]] = ContextVar("activeContainer")
    """Context variable to store the active container."""

    def __init__(
        self,
        config: t.Optional[t.Mapping[str, t.Any]] = None,
        namespace: t.Optional[str] = None,
        parent: t.Optional["Container"] = None,
    ) -> None:
        """Initialize the context with a configuration loader.

        Args:
            config: Configuration to use for the context.
            namespace: Namespace to use for the context.
            parent: Parent context to inherit dependencies from.
        """
        self._dependencies: t.Dict[t.Tuple[t.Optional[str], str], t.Any] = {}
        self._factories: t.Dict[
            t.Tuple[t.Optional[str], str], t.Tuple[t.Callable[..., t.Any], bool]
        ] = {}
        self._singletons: t.Dict[t.Tuple[t.Optional[str], str], t.Any] = {}
        self._resolving: t.Set[t.Tuple[t.Optional[str], str]] = set()
        self._lock = threading.RLock()
        self._exit_stack = contextlib.ExitStack()
        self._call_stack_depth = 0
        self._config = ConfigBox(config or {})
        self._tokens = []
        self.namespace = namespace
        self.parent = parent

    @property
    def config(self) -> ConfigBox:
        """Return the read-only configuration for the context.

        Returns:
            Read-only configuration box
        """
        return self._config

    @config.setter
    def config(self, value: t.Mapping[str, t.Any]) -> None:
        """Set a new read-only configuration for the context.

        Args:
            value: New configuration
        """
        self._config = ConfigBox(value)

    @property
    def config_readonly(self) -> MappingProxyType[str, t.Any]:
        """Return the read-only configuration for the context.

        Returns:
            Read-only configuration box
        """
        return MappingProxyType(self._config)

    def add(self, name: str, instance: t.Any, namespace: t.Optional[str] = None) -> None:
        """Register a dependency instance.

        Args:
            name: Name of the dependency.
            instance: Dependency instance to register.
        """
        with self._lock:
            self._dependencies[(namespace or self.namespace, name)] = instance

    def add_factory(
        self,
        name: str,
        factory: t.Callable[..., t.Any],
        singleton: bool = True,
        namespace: t.Optional[str] = None,
    ) -> None:
        """Register a dependency factory.

        Args:
            name: Name of the dependency.
            factory: Dependency factory function.
            singleton: Whether the dependency should be a singleton.
            namespace: Namespace to use for the dependency.
        """
        key = (namespace or self.namespace, name)
        with self._lock:
            self._factories[key] = (factory, singleton)
            if singleton and key in self._singletons:
                del self._singletons[key]

    def get(
        self,
        name: str,
        default: t.Optional[t.Any] = ...,
        namespace: t.Optional[str] = None,
    ) -> t.Any:
        """Resolve a dependency by name, handling recursive dependencies.

        Args:
            name: Name of the dependency.
            default: Default value to return if the dependency is not found.
            namespace: Namespace to use for the dependency.

        Raises:
            RuntimeError: If a dependency cycle is detected.

        Returns:
            Resolved dependency or default value.
        """
        key = (namespace or self.namespace, name)
        if key in self._dependencies:
            return self._dependencies[key]
        elif key in self._factories:
            if key in self._resolving:
                cycle = " -> ".join([f"{ns}:{n}" for ns, n in list(self._resolving) + [key]])
                raise DependencyCycleError(f"Dependency cycle detected: {cycle}")
            self._resolving.add(key)
            try:
                factory, singleton = self._factories[key]
                if singleton and key in self._singletons:
                    return self._singletons[key]
                factory = self.inject_deps(factory)
                result = factory()
                if isinstance(result, contextlib.AbstractContextManager):
                    if singleton:
                        result = result.__enter__()

                        def _cleanup() -> None:
                            result.__exit__(*sys.exc_info())

                        atexit.register(_cleanup)
                    else:
                        result = self._exit_stack.enter_context(result)
                elif inspect.iscoroutine(result):
                    try:
                        loop = asyncio.get_running_loop()
                    except RuntimeError:
                        loop = None
                    if loop and loop.is_running():
                        result = asyncio.run_coroutine_threadsafe(result, loop).result()
                    else:
                        result = asyncio.run(result)
                if singleton:
                    self._singletons[key] = result
                return result
            finally:
                self._resolving.remove(key)
        elif self.parent:
            return self.parent.get(name, default, namespace)
        else:
            if default is not ...:
                return default
            raise DependencyNotFoundError(
                f"Dependency '{name}' not found in namespace '{namespace or self.namespace}'. "
                f"Available dependencies: {list(self)}"
            )

    def drop(self, name: str, namespace: t.Optional[str] = None) -> None:
        """Drop a singleton dependency.

        Args:
            name: Name of the dependency.
            namespace: Namespace to use for the dependency.

        Raises:
            DependencyNotFoundError: If the dependency is not found.
        """
        key = (namespace or self.namespace, name)
        with self._lock:
            if key in self._dependencies:
                del self._dependencies[key]
            elif key in self._factories:
                del self._factories[key]
                self._singletons.pop(key, None)
            else:
                raise DependencyNotFoundError(
                    f"Dependency '{name}' not found in namespace '{self.namespace}'"
                )

    def __getitem__(self, name: str) -> t.Any:
        """Allow dictionary-like access to dependencies.

        Args:
            name: Name of the dependency.

        Raises:
            DependencyNotFoundError: If the dependency is not found.

        Returns:
            Resolved dependency.
        """
        return self.get(name)

    def __setitem__(self, name: str, value: t.Any) -> None:
        """Set a dependency instance.

        Args:
            name: Name of the dependency.
            value: Value to set as the dependency.

        Returns:
            Resolved dependency.
        """
        if callable(value):
            self.add_factory(name, value)
        else:
            self.add(name, value)

    def __delitem__(self, name: str) -> None:
        """Delete a dependency.

        Args:
            name: Name of the dependency.

        Raises:
            DependencyNotFoundError: If the dependency is not found.

        Returns:
            Resolved dependency.
        """
        self.drop(name)

    def __iter__(self) -> t.Iterator[str]:
        """Iterate over the dependency names."""
        seen = set()
        for _, name in set(self._dependencies.keys()).union(self._factories.keys()):
            if name not in seen:
                yield name
                seen.add(name)
        if self.parent is not None:
            for name in self.parent:
                if name not in seen:
                    yield name
                    seen.add(name)

    def __len__(self) -> int:
        """Return the number of dependencies."""
        dep_count = len(set(self._dependencies.keys()).union(self._factories.keys()))
        if self.parent is not None:
            dep_count += len(self.parent)
        return dep_count

    def __contains__(self, name: object) -> bool:
        """Check if a dependency is registered.

        Args:
            name: Name of the dependency.

        Returns:
            True if the dependency is registered, False otherwise
        """
        key = (self.namespace, name)
        exists = key in self._dependencies or key in self._factories
        if not exists and self.parent is not None:
            return name in self.parent
        return exists

    def inject_deps(self, func: t.Callable[..., T]) -> t.Callable[..., T]:
        """Decorator to inject dependencies into functions based on parameter names.

        We also inject the context as a parameter with the name 'C'. This is allows
        access to the DI container and configuration within the function.

        Args:
            func: Function to decorate.

        Returns:
            Decorated function that injects dependencies based on parameter names.

        Example:
            @context.inject_deps
            def my_function(db_connection, config):
                # db_connection and config are injected based on their names
                pass
        """
        sig = inspect.signature(func)

        @wraps(func)
        def wrapper(*args, **kwargs) -> T:
            bound_args = sig.bind_partial(*args, **kwargs)
            for name, p in sig.parameters.items():
                if name not in bound_args.arguments:
                    if name == CONTEXT_PARAM_NAME:
                        bound_args.arguments[name] = self
                    elif name in self:
                        bound_args.arguments[name] = self.get(name)
                    elif p.default is injected or p.default is ...:
                        raise DependencyNotFoundError(
                            f"Required dependency '{name}' not found in namespace '{self.namespace}' while calling '{func.__name__}'. "
                        )
            self._call_stack_depth += 1
            try:
                rv = func(*bound_args.args, **bound_args.kwargs)
                return rv
            finally:
                self._call_stack_depth -= 1
                if self._call_stack_depth == 0:
                    self._exit_stack.close()

        return wrapper

    wire = inject_deps  # Alias for inject_deps

    def __call__(self, func: t.Callable[..., T]) -> t.Callable[..., T]:
        """Allow the context to be used as a decorator.

        Args:
            func: Function to decorate.

        Returns:
            Decorated function that injects dependencies based on parameter names.
        """
        return self.inject_deps(func)

    @t.overload
    def register_dep(
        self,
        func: t.Callable[P, T],
        /,
    ) -> t.Callable[P, T]:
        """Decorator to register a singleton dependency in the global context.

        Args:
            func: Function to register.

        Returns:
            Function that registers the dependency in the global context.
        """

    @t.overload
    def register_dep(
        self,
        name: t.Optional[str] = None,
        /,
        *,
        singleton: bool = True,
        namespace: t.Optional[str] = None,
    ) -> t.Callable[[t.Callable[P, T]], t.Callable[P, T]]:
        """Decorator to register a dependency in the global context.

        Args:
            name: Name of the dependency, if not provided, the function name is used.
            singleton: Whether the dependency should be a singleton.
            namespace: Namespace to use for the dependency.

        Returns:
            Decorator function that registers the dependency in the global context.
        """

    def register_dep(
        self,
        name_or_func: t.Union[None, str, t.Callable[P, T]] = None,
        /,
        singleton: bool = True,
        namespace: t.Optional[str] = None,
    ) -> t.Union[t.Callable[P, T], t.Callable[[t.Callable[P, T]], t.Callable[P, T]]]:
        """Decorator to register a dependency in the global context.

        Args:
            name_or_func: Name of the dependency or function to register.
            singleton: Whether the dependency should be a singleton.
            namespace: Namespace to use for the dependency.

        Returns:
            Decorator function that registers the dependency in the global context.
        """

        def decorator(func: t.Callable[P, T]) -> t.Callable[P, T]:
            nonlocal name_or_func
            name = name_or_func if isinstance(name_or_func, str) else func.__name__
            self.add_factory(name or func.__name__, func, singleton=singleton, namespace=namespace)
            return func

        if callable(name_or_func):
            return decorator(name_or_func)

        return decorator

    def __enter__(self) -> "Container":
        self._tokens.append(active_container.set(self))
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        active_container.reset(self._tokens.pop())
        self._exit_stack.__exit__(exc_type, exc_value, traceback)

    def combine(self, other: "Container") -> "Container":
        """Combine this context with another, returning a new context with merged configurations and dependencies.

        Args:
            other: Context to combine with.

        Returns:
            New context with merged configurations and dependencies
        """
        combined_context = self.__class__(
            {**self.config, **other.config}, namespace=self.namespace, parent=self
        )
        combined_context._dependencies.update(other._dependencies)
        combined_context._factories.update(other._factories)
        combined_context._singletons.update(other._singletons)
        return combined_context

    def reset(self) -> None:
        """Reset the context to its initial state."""
        self._dependencies.clear()
        self._factories.clear()
        self._singletons.clear()
        self._resolving.clear()


active_container = Container.active
"""Context variable to store the active container."""


@t.overload
def register_dep(
    func: t.Callable[P, T],
    /,
) -> t.Callable[P, T]:
    """Decorator to register a singleton dependency in the global context.

    Args:
        func: Function to register.

    Returns:
        Function that registers the dependency in the global context.
    """


@t.overload
def register_dep(
    name: t.Optional[str] = None,
    /,
    *,
    singleton: bool = True,
    namespace: t.Optional[str] = None,
) -> t.Callable[[t.Callable[P, T]], t.Callable[P, T]]:
    """Decorator to register a dependency in the global context.

    Args:
        name: Name of the dependency, if not provided, the function name is used.
        singleton: Whether the dependency should be a singleton.
        namespace: Namespace to use for the dependency.

    Returns:
        Decorator function that registers the dependency in the global context.
    """


def register_dep(
    name_or_func: t.Union[None, str, t.Callable[P, T]] = None,
    /,
    singleton: bool = True,
    namespace: t.Optional[str] = None,
) -> t.Union[t.Callable[P, T], t.Callable[[t.Callable[P, T]], t.Callable[P, T]]]:
    """Decorator to register a dependency in the global context.

    Args:
        name_or_func: Name of the dependency or function to register.
        singleton: Whether the dependency should be a singleton.
        namespace: Namespace to use for the dependency.

    Returns:
        Decorator function that registers the dependency in the global context.
    """

    def decorator(func: t.Callable[P, T]) -> t.Callable[P, T]:
        nonlocal name_or_func
        name = name_or_func if isinstance(name_or_func, str) else func.__name__
        ctx = active_container.get()
        ctx.add_factory(name or func.__name__, func, singleton=singleton, namespace=namespace)
        return func

    if callable(name_or_func):
        return decorator(name_or_func)

    return decorator


@t.overload
def inject_deps(func: t.Callable[..., T], /) -> t.Callable[..., T]: ...


@t.overload
def inject_deps(
    *, late_bind: bool = True
) -> t.Callable[[t.Callable[..., T]], t.Callable[..., T]]: ...


def inject_deps(func: t.Optional[t.Callable[..., T]] = None, /, *, late_bind: bool = True):
    """Decorator to inject dependencies into functions based on parameter names."""

    def decorator(f: t.Callable[..., T]) -> t.Callable[..., T]:
        container = None if late_bind else active_container.get()

        @wraps(f)
        def wrapper(*args, **kwargs):
            nonlocal container
            if container is None:
                container = active_container.get()
            return container(f)(*args, **kwargs)

        return wrapper

    if func is None:
        return decorator
    return decorator(func)