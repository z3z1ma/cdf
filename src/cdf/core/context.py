"""Context module for dependency injection and configuration management."""

import ast
import asyncio
import collections
import importlib.util
import inspect
import io
import json
import linecache
import os
import re
import string
import sys
import threading
import typing as t
from contextvars import ContextVar
from functools import wraps
from pathlib import Path

import yaml
from box import Box

if sys.version_info >= (3, 11):
    import tomllib
else:
    import tomli as tomllib

if sys.version_info >= (3, 9):
    from typing import ParamSpec
else:
    from typing_extensions import ParamSpec


T = t.TypeVar("T")
P = ParamSpec("P")

ConfigurationSource = t.Union[
    str, Path, t.Mapping[str, t.Any], t.Callable[[], "ConfigurationSource"]
]

_CONTEXT_PARAM_NAME = "C"

__all__ = [
    "Context",
    "ConfigurationSource",
    "ConverterBox",
    "SimpleConfigurationLoader",
    "register_dep",
    "active_context",
    "add_custom_converter",
    "_get_converter",
    "_remove_converter",
]


def _to_bool(value: str) -> bool:
    """Convert a string to a boolean value."""
    return value.lower() in ("true", "yes", "1")


def _make_eval_func(type_: t.Type):
    """Create a function to evaluate a py literal string with type assertion."""

    def _eval(value: str) -> t.Any:
        v = ast.literal_eval(value)
        if not isinstance(v, type_):
            raise ValueError(f"Value is not of type {type_}")
        return v

    return _eval


_CONVERTERS = {
    "json": json.loads,
    "int": int,
    "float": float,
    "str": str,
    "bool": _to_bool,
    "path": os.path.abspath,
    "dict": _make_eval_func(dict),
    "list": _make_eval_func(list),
    "tuple": _make_eval_func(tuple),
    "set": _make_eval_func(set),
    "resolve": None,
}
"""Converters for configuration values."""

_CONVERTER_PATTERN = re.compile(r"@(\w+) ", re.IGNORECASE)
"""Pattern to match converters in a string."""


def add_custom_converter(name: str, converter: t.Callable[[str], t.Any]) -> None:
    """Add a custom converter to the configuration system."""
    if name in _CONVERTERS:
        raise ValueError(f"Converter {name} already exists.")
    _CONVERTERS[name] = converter


def _get_converter(name: str) -> t.Callable[[str], t.Any]:
    """Get a custom converter from the configuration system."""
    return _CONVERTERS[name]


def _remove_converter(name: str) -> None:
    """Remove a custom converter from the configuration system."""
    if name not in _CONVERTERS:
        raise ValueError(f"Converter {name} does not exist.")
    del _CONVERTERS[name]


def _expand_env_vars(template: str, **env_overrides: t.Any) -> str:
    """Resolve environment variables in the format ${VAR} or $VAR."""
    return string.Template(template).substitute(env_overrides, **os.environ)


def _read_config_file(
    path: t.Union[str, Path],
    mode: str = "r",
    parser: t.Callable[[str], t.Any] = json.loads,
    **env_overrides: t.Any,
) -> t.Any:
    """Read a file from the given path and parse it using the specified parser."""
    with open(path, mode=mode) as f:
        rendered = _expand_env_vars(f.read(), **env_overrides)
    return parser(rendered)


class ConverterBox(Box):
    """Box that applies @ converters to configuration values."""

    def __getitem__(self, item: t.Any, _ignore_default: bool = False) -> t.Any:
        value = super().__getitem__(item, _ignore_default)
        if isinstance(value, str):
            return self._apply_converters(value)
        return value

    def values(self) -> t.ValuesView[t.Any]:  # type: ignore
        return t.cast(
            t.ValuesView[t.Any], map(self._apply_converters, super().values())
        )

    def _apply_converters(self, data: t.Any) -> t.Any:
        """Apply converters to a configuration value.

        Converters are prefixed with @. The following default converters are supported:
        - json: Convert to JSON object
        - int: Convert to integer
        - float: Convert to float
        - str: Convert to string
        - bool: Convert to boolean
        - path: Convert to absolute path
        - dict: Convert to dictionary
        - list: Convert to list
        - tuple: Convert to tuple
        - set: Convert to set
        - resolve: Resolve value from partial configuration

        Args:
            data: Configuration value to apply converters to.

        Raises:
            ValueError: If an unknown converter is used or if a conversion fails.

        Returns:
            Converted configuration value.
        """
        if not isinstance(data, str):
            raise ValueError("Value must be a string")
        data = _expand_env_vars(data)
        converters = _CONVERTER_PATTERN.findall(data)
        if len(converters) == 0:
            return data
        base_v = _CONVERTER_PATTERN.sub("", data).lstrip()
        if not base_v:
            return None
        transformed_v = base_v
        for converter in reversed(converters):
            try:
                if converter.lower() == "resolve":
                    try:
                        transformed_v = self[transformed_v]
                    except KeyError as e:
                        raise ValueError(f"Key not found in resolver: {e}") from e
                else:
                    transformed_v = _CONVERTERS[converter.lower()](transformed_v)
            except KeyError as e:
                raise ValueError(f"Unknown converter: {converter}") from e
            except Exception as e:
                raise ValueError(f"Failed to convert value: {e}") from e
        return transformed_v


def _merge_configs(*configs: Box) -> Box:
    """Combine multiple configuration Boxes using merge_update."""
    merged = ConverterBox(box_dots=True)
    for config in configs:
        merged.merge_update(config)
    return merged


def _scope_configs(*configs: Box) -> Box:
    """Combine multiple configuration Boxes via ChainMap to provide scope-based resolution."""
    return ConverterBox(collections.ChainMap(*configs), box_dots=True)


class DependencyCycleError(RuntimeError):
    """Raised when a dependency cycle is detected."""


class DependencyNotFoundError(KeyError):
    """Raised when a dependency is not found in the context."""


class SimpleConfigurationLoader:
    """Loads configuration from multiple sources and merges them using a resolution strategy."""

    def __init__(
        self,
        *sources: ConfigurationSource,
        resolution_strategy: t.Literal["merge", "scope"] = "merge",
        include_env: bool = True,
    ) -> None:
        """Initialize the configuration loader with given sources.

        Args:
            sources: Configuration sources to load.
            resolution_strategy: Strategy to use for merging configurations
            - "merge": Merge all configurations into a single Box
            - "scope": Combine configurations into a ChainMap for scope-based resolution
            include_env: Whether to include environment variables

        Raises:
            ValueError: If an unsupported resolution strategy is provided
        """
        if resolution_strategy not in ("merge", "scope"):
            raise ValueError(f"Unsupported resolution strategy: {resolution_strategy}")
        self.sources = sources
        if include_env:
            self.sources += (dict(os.environ),)
        self._config = None
        self._resolver = (
            _merge_configs if resolution_strategy == "merge" else _scope_configs
        )

    def add_source(self, source: ConfigurationSource) -> t.Mapping[str, t.Any]:
        """Add a configuration source to the loader.

        Args:
            source: Configuration source to add.
        """
        self.sources += (source,)
        return self.load()

    def load(self) -> Box:
        """Load and merge configurations from all sources."""
        configs = [Box(self._load(source), box_dots=True) for source in self.sources]
        self._config = self._resolver(*configs)
        return self._config

    @staticmethod
    def _load(source: ConfigurationSource) -> t.Mapping[str, t.Any]:
        """Load configuration from a single source.

        Args:
            source: Configuration source to load.

        Returns:
            Configuration as a dictionary.
        """
        if callable(source):
            return SimpleConfigurationLoader._load(source())
        elif isinstance(source, dict):
            return source
        elif isinstance(source, (str, Path)):
            path = Path(source)
            if not path.exists():
                return {}
            if path.suffix == ".json":
                return _read_config_file(path, parser=json.loads)
            elif path.suffix in (".yaml", ".yml"):
                return _read_config_file(
                    path, parser=lambda s: yaml.safe_load(io.StringIO(s))
                )
            elif path.suffix == ".toml":
                return _read_config_file(path, parser=tomllib.loads)
            else:
                raise ValueError(f"Unsupported file format: {path.suffix}")
        else:
            raise TypeError(f"Invalid config source: {source}")


class Context(t.MutableMapping[str, t.Any]):
    """Provides access to configuration and acts as a DI container with dependency resolution."""

    loader_type = SimpleConfigurationLoader

    def __init__(
        self,
        loader: SimpleConfigurationLoader,
        namespace: t.Optional[str] = None,
        parent: t.Optional["Context"] = None,
    ) -> None:
        """Initialize the context with a configuration loader.

        Args:
            loader: Configuration loader to use for loading configuration.
            namespace: Namespace to use for the context.
            parent: Parent context to inherit dependencies from.
        """
        self._loader = loader
        self._config: t.Optional[Box] = None
        self._dependencies: t.Dict[t.Tuple[t.Optional[str], str], t.Any] = {}
        self._factories: t.Dict[
            t.Tuple[t.Optional[str], str], t.Tuple[t.Callable[..., t.Any], bool]
        ] = {}
        self._singletons: t.Dict[t.Tuple[t.Optional[str], str], t.Any] = {}
        self._resolving: t.Set[t.Tuple[t.Optional[str], str]] = set()
        self._lock = threading.RLock()
        self.namespace = namespace
        self.parent = parent

    @property
    def config(self) -> Box:
        """Lazily load and return the configuration as a Box."""
        if self._config is None:
            self._config = self._loader.load()
        return self._config

    def add(
        self, name: str, instance: t.Any, namespace: t.Optional[str] = None
    ) -> None:
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
                cycle = " -> ".join(
                    [f"{ns}:{n}" for ns, n in list(self._resolving) + [key]]
                )
                raise DependencyCycleError(f"Dependency cycle detected: {cycle}")
            self._resolving.add(key)
            try:
                factory, singleton = self._factories[key]
                if singleton and key in self._singletons:
                    return self._singletons[key]
                factory = self.inject_dependencies(factory)
                result = factory()
                if inspect.iscoroutine(result):
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
            return self.parent.get(name, default, namespace or self.namespace)
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
        return (
            name
            for (_, name) in set(self._dependencies.keys()).union(
                self._factories.keys()
            )
        )

    def __len__(self) -> int:
        """Return the number of dependencies."""
        return len(set(self._dependencies.keys()).union(self._factories.keys()))

    def __contains__(self, name: object) -> bool:
        """Check if a dependency is registered.

        Args:
            name: Name of the dependency.

        Returns:
            True if the dependency is registered, False otherwise
        """
        key = (self.namespace, name)
        return key in self._dependencies or key in self._factories

    def inject_dependencies(self, func: t.Callable[..., T]) -> t.Callable[..., T]:
        """Decorator to inject dependencies into functions based on parameter names.

        We also inject the context as a parameter with the name 'C'. This is allows
        access to the DI container and configuration within the function.

        Args:
            func: Function to decorate.

        Returns:
            Decorated function that injects dependencies based on parameter names.

        Example:
            @context.inject_dependencies
            def my_function(db_connection, config):
                # db_connection and config are injected based on their names
                pass
        """
        sig = inspect.signature(func)

        @wraps(func)
        def wrapper(*args, **kwargs) -> T:
            bound_args = sig.bind_partial(*args, **kwargs)
            for name, _ in sig.parameters.items():
                if name not in bound_args.arguments:
                    if name == _CONTEXT_PARAM_NAME:
                        bound_args.arguments[name] = self
                    elif name in self:
                        bound_args.arguments[name] = self.get(name)
            return func(*bound_args.args, **bound_args.kwargs)

        return wrapper

    wire = inject_dependencies  # Alias for inject_dependencies

    def __call__(self, func: t.Callable[..., T]) -> t.Callable[..., T]:
        """Allow the context to be used as a decorator.

        Args:
            func: Function to decorate.

        Returns:
            Decorated function that injects dependencies based on parameter names.
        """
        return self.inject_dependencies(func)

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
            self.add_factory(
                name or func.__name__, func, singleton=singleton, namespace=namespace
            )
            return func

        if callable(name_or_func):
            return decorator(name_or_func)

        return decorator

    def __enter__(self) -> "Context":
        self._token = active_context.set(self)
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        active_context.reset(self._token)

    def reload_config(self):
        """Reload the configuration from the sources."""
        self._config = self._loader.load()

    def combine(self, other: "Context") -> "Context":
        """Combine this context with another, returning a new context with merged configurations and dependencies.

        Args:
            other: Context to combine with.

        Returns:
            New context with merged configurations and dependencies
        """
        combined_loader = self.loader_type(
            *self._loader.sources,
            *other._loader.sources,
            resolution_strategy="merge",
        )
        combined_context = self.__class__(
            loader=combined_loader, namespace=self.namespace, parent=self
        )
        combined_context._dependencies.update(other._dependencies)
        combined_context._factories.update(other._factories)
        combined_context._singletons.update(other._singletons)
        return combined_context

    def load_dependencies_from_config(self):
        """Load plugins specified in the configuration under 'dependency_paths'."""
        dep_paths = self.config.get("dependency_paths", [])
        with self:
            linecache.clearcache()
            for path_str in dep_paths:
                path = Path(path_str)
                if path.is_dir():
                    for file in path.glob("*.py"):
                        module_name = file.stem
                        spec = importlib.util.spec_from_file_location(module_name, file)
                        module = importlib.util.module_from_spec(spec)  # type: ignore
                        spec.loader.exec_module(module)  # type: ignore
                else:
                    raise ValueError(
                        f"Plugin path '{path}' is not a directory or does not exist."
                    )


active_context: ContextVar[Context] = ContextVar("active_context")
"""Stores the active context for the current execution context."""


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
        ctx = active_context.get()
        ctx.add_factory(
            name or func.__name__, func, singleton=singleton, namespace=namespace
        )
        return func

    if callable(name_or_func):
        return decorator(name_or_func)

    return decorator
