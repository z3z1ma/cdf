"""Context module for dependency injection and configuration management."""

import ast
import collections
import inspect
import io
import json
import os
import re
import string
import sys
import typing as t
from contextvars import ContextVar
from functools import wraps
from pathlib import Path

if sys.version_info >= (3, 11):
    import tomllib
else:
    import tomli as tomllib

import yaml
from box import Box

ConfigurationSource = t.Union[
    str, Path, t.Mapping[str, t.Any], t.Callable[[], "ConfigurationSource"]
]

_CONTEXT_PARAM_NAME = "context"
_CONFIG_PARAM_NAME = "configuration"

__all__ = [
    "Context",
    "ConfigurationSource",
    "ConverterBox",
    "SimpleConfigurationLoader",
    "dependency",
    "active_context",
    "add_custom_converter",
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

    def load(self) -> Box:
        """Load and merge configurations from all sources."""
        if self._config is not None:
            return self._config
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

    def __init__(self, config_loader: SimpleConfigurationLoader) -> None:
        """Initialize the context with a configuration loader.

        Args:
            config_loader: Configuration loader to use for loading configuration.
        """
        self._config_loader = config_loader
        self._config: t.Optional[Box] = None
        self._dependencies: t.Dict[str, t.Any] = {}
        self._factories: t.Dict[str, t.Tuple[t.Callable[..., t.Any], bool]] = {}
        self._singletons: t.Dict[str, t.Any] = {}
        self._resolving: t.Set[str] = set()

    @property
    def config(self) -> Box:
        """Lazily load and return the configuration as a Box."""
        if self._config is None:
            self._config = self._config_loader.load()
        return self._config

    def add(self, name: str, instance: t.Any) -> None:
        """Register a dependency instance.

        Args:
            name: Name of the dependency.
            instance: Dependency instance to register.
        """
        self._dependencies[name] = instance

    def add_factory(
        self, name: str, factory: t.Callable[..., t.Any], singleton: bool = True
    ) -> None:
        """Register a dependency factory.

        Args:
            name: Name of the dependency.
            factory: Dependency factory function.
            singleton: Whether the dependency should be a singleton.
        """
        self._factories[name] = (factory, singleton)
        if singleton and name in self._singletons:
            del self._singletons[name]

    def get(self, name: str, default: t.Optional[t.Any] = None) -> t.Any:
        """Resolve a dependency by name, handling recursive dependencies.

        Args:
            name: Name of the dependency.
            default: Default value to return if the dependency is not found.

        Raises:
            RuntimeError: If a dependency cycle is detected.

        Returns:
            Resolved dependency or default value.
        """
        if name in self._dependencies:
            return self._dependencies[name]
        elif name in self._factories:
            if name in self._resolving:
                raise RuntimeError(
                    f"Dependency cycle detected: {' -> '.join(list(self._resolving) + [name])}"
                )
            self._resolving.add(name)
            try:
                factory, singleton = self._factories[name]
                wrapped_factory = self.inject_dependencies(factory)
                if singleton:
                    if name not in self._singletons:
                        self._singletons[name] = wrapped_factory()
                    return self._singletons[name]
                else:
                    return wrapped_factory()
            finally:
                self._resolving.remove(name)
        else:
            return default

    def __getitem__(self, name: str) -> t.Any:
        """Allow dictionary-like access to dependencies.

        Args:
            name: Name of the dependency.

        Raises:
            KeyError: If the dependency is not found.

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
            KeyError: If the dependency is not found.

        Returns:
            Resolved dependency.
        """
        if name in self._dependencies:
            del self._dependencies[name]
        elif name in self._factories:
            del self._factories[name]
            self._singletons.pop(name, None)
        else:
            raise KeyError(f"Dependency '{name}' not found")

    def __iter__(self) -> t.Iterator[str]:
        """Iterate over the dependency names."""
        return iter(set(self._dependencies.keys()).union(self._factories.keys()))

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
        return name in self._dependencies or name in self._factories

    def inject_dependencies(self, func: t.Callable) -> t.Callable:
        """Decorator to inject dependencies into functions based on parameter names.

        Args:
            func: Function to decorate.

        Returns:
            Decorated function that injects dependencies based on parameter names.
        """
        sig = inspect.signature(func)

        @wraps(func)
        def wrapper(*args, **kwargs):
            bound_args = sig.bind_partial(*args, **kwargs)
            for name, _ in sig.parameters.items():
                if name not in bound_args.arguments:
                    if name == _CONTEXT_PARAM_NAME:
                        bound_args.arguments[name] = self
                    elif name == _CONFIG_PARAM_NAME:
                        bound_args.arguments[name] = self.config
                    elif name in self:
                        bound_args.arguments[name] = self.get(name)
            return func(*bound_args.args, **bound_args.kwargs)

        return wrapper

    def __call__(self, func: t.Callable) -> t.Callable:
        """Allow the context to be used as a decorator.

        Args:
            func: Function to decorate.

        Returns:
            Decorated function that injects dependencies based on parameter names.
        """
        return self.inject_dependencies(func)

    def dependency(
        self, name: t.Optional[str] = None, /, singleton: bool = True
    ) -> t.Callable:
        """Decorator to register a dependency in the context.

        Args:
            name: Name of the dependency.
            singleton: Whether the dependency should be a singleton.

        Returns:
            Decorator function that registers the dependency in the context.
        """

        def decorator(func: t.Callable[..., t.Any]) -> t.Callable[..., t.Any]:
            nonlocal name
            if name is None:
                name = func.__name__
            self.add_factory(name, func, singleton=singleton)
            return func

        return decorator


active_context: ContextVar[Context] = ContextVar("active_context")
"""Stores the active context for the current execution context."""


def dependency(name: t.Optional[str] = None, /, singleton: bool = True):
    """Decorator to register a dependency in the global context.

    Args:
        name: Name of the dependency.
        singleton: Whether the dependency should be a singleton.

    Returns:
        Decorator function that registers the dependency in the global context.
    """

    def decorator(func: t.Callable[..., t.Any]) -> t.Callable[..., t.Any]:
        nonlocal name
        if name is None:
            name = func.__name__
        ctx = active_context.get()
        ctx.add_factory(name, func, singleton=singleton)
        return func

    return decorator


if __name__ == "__main__":
    ctx = Context(
        config_loader=SimpleConfigurationLoader(
            # Example configuration sources, dictionary, file path, callable
            # with converters and environment variable expansion
            {"name": "${USER}", "age": 30},
            {"model": "SVM", "num_iter": "@int 35"},
            lambda: {"processor": "add_one", "seq": "@tuple (1,2,3)"},
            lambda: {"model_A": "@float @resolve age"},
            "pyproject.toml",
        )
    )
    # Access configuration by index or attr (configs are merged)
    print(ctx.config["name"], ctx.config.age)
    # Num iter is converted to int via converter
    print(ctx.config.num_iter, type(ctx.config.num_iter))
    # A file path is used to load configuration based on extension
    print(ctx.config["project"])
    # A @resolve converter is used to self-reference a configuration value
    print(ctx.config.model_A)
    # Env vars are made available (case-sensitive)
    print(ctx.config.USER)

    active_context.set(ctx)

    @dependency("bar")
    def foo(context: Context, config: dict) -> bool:
        assert context is ctx, "Context is not the same"
        assert config is ctx.config, "Config is not the same"
        return True

    # Inject dependencies
    print(ctx["bar"])
