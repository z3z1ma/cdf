import ast
import functools
import inspect
import json
import os
import re
import string
import typing as t
from collections import ChainMap
from contextlib import suppress
from pathlib import Path

import ruamel.yaml as yaml
import tomlkit
from dynaconf.vendor.box import Box
from typing_extensions import ParamSpec

import cdf.core.logger as logger
from cdf.types import M

T = t.TypeVar("T")
P = ParamSpec("P")


def load_file(path: Path) -> M.Result[t.Dict[str, t.Any], Exception]:
    """Load a configuration from a file path.

    Args:
        path: The file path.

    Returns:
        A Result monad with the configuration dictionary if the file format is JSON, YAML or TOML.
        Otherwise, a Result monad with an error.
    """
    if path.suffix == ".json":
        return _load_json(path)
    if path.suffix in (".yaml", ".yml"):
        return _load_yaml(path)
    if path.suffix == ".toml":
        return _load_toml(path)
    return M.error(ValueError("Invalid file format, must be JSON, YAML or TOML"))


def _load_json(path: Path) -> M.Result[t.Dict[str, t.Any], Exception]:
    """Load a configuration from a JSON file.

    Args:
        path: The file path to a valid JSON document.

    Returns:
        A Result monad with the configuration dictionary if the file format is JSON. Otherwise, a
        Result monad with an error.
    """
    try:
        return M.ok(json.loads(path.read_text()))
    except Exception as e:
        return M.error(e)


def _load_yaml(path: Path) -> M.Result[t.Dict[str, t.Any], Exception]:
    """Load a configuration from a YAML file.

    Args:
        path: The file path to a valid YAML document.

    Returns:
        A Result monad with the configuration dictionary if the file format is YAML. Otherwise, a
        Result monad with an error.
    """
    try:
        yaml_ = yaml.YAML()
        return M.ok(yaml_.load(path))
    except Exception as e:
        return M.error(e)


def _load_toml(path: Path) -> M.Result[t.Dict[str, t.Any], Exception]:
    """Load a configuration from a TOML file.

        Args:
    path: The file path to a valid TOML document.

        Returns:
            A Result monad with the configuration dictionary if the file format is TOML. Otherwise, a
            Result monad with an error.
    """
    try:
        return M.ok(tomlkit.loads(path.read_text()).unwrap())
    except Exception as e:
        return M.error(e)


def _to_bool(value: str) -> bool:
    """Convert a string to a boolean."""
    return value.lower() in ["true", "1", "yes"]


def _resolve_template(template: str, **overrides: t.Any) -> str:
    """Resolve a template string using environment variables."""
    return string.Template(template).substitute(overrides, **os.environ)


_CONVERTERS = {
    "json": json.loads,
    "int": int,
    "float": float,
    "str": str,
    "bool": _to_bool,
    "path": os.path.abspath,
    "dict": ast.literal_eval,
    "list": ast.literal_eval,
    "tuple": ast.literal_eval,
    "set": ast.literal_eval,
    "path": os.path.abspath,
}
"""Converters for configuration values."""

_CONVERTER_PATTERN = re.compile(r"@(\w+) ", re.IGNORECASE)
"""Pattern to match converters in a string."""


def add_custom_converter(name: str, converter: t.Callable[[str], t.Any]) -> None:
    """Add a custom converter to the configuration system."""
    if name in _CONVERTERS:
        raise ValueError(f"Converter {name} already exists.")
    _CONVERTERS[name] = converter


def get_converter(name: str) -> t.Callable[[str], t.Any]:
    """Get a custom converter from the configuration system."""
    return _CONVERTERS[name]


def remove_converter(name: str) -> None:
    """Remove a custom converter from the configuration system."""
    if name not in _CONVERTERS:
        raise ValueError(f"Converter {name} does not exist.")
    del _CONVERTERS[name]


def apply_converters(input_value: t.Any, **overrides: t.Any) -> t.Any:
    """Apply converters to a string."""
    if not isinstance(input_value, str):
        return input_value
    expanded_value = _resolve_template(input_value, **overrides)
    converters = _CONVERTER_PATTERN.findall(expanded_value)
    if len(converters) == 0:
        return expanded_value
    base_value = _CONVERTER_PATTERN.sub("", expanded_value).lstrip()
    if not base_value:
        return None
    transformed_value = base_value
    for converter in reversed(converters):
        try:
            transformed_value = _CONVERTERS[converter.lower()](transformed_value)
        except KeyError as e:
            raise ValueError(f"Unknown converter: {converter}") from e
        except Exception as e:
            raise ValueError(f"Failed to convert value: {e}") from e
    return transformed_value


def _to_box(mapping: t.Mapping[str, t.Any]) -> Box:
    """Convert a mapping to a standardized Box."""
    return Box(mapping, box_dots=True)


class _ConfigScopes(t.NamedTuple):
    """A struct to store named configuration scopes by precedence."""

    explicit: Box
    """User-provided configuration passed as a dictionary."""
    environment: Box
    """Environment-specific configuration loaded from a file."""
    baseline: Box
    """Configuration loaded from a base config file."""

    def resolve(self) -> Box:
        """Resolve the configuration scopes."""
        output = self.baseline
        output.merge_update(self.environment)
        output.merge_update(self.explicit)
        return output


ConfigSource = t.Union[str, Path, t.Mapping[str, t.Any]]


class ConfigLoader:
    """Load configuration from multiple sources."""

    def __init__(
        self,
        *sources: ConfigSource,
        environment: str = "dev",
        deferred: bool = False,
    ) -> None:
        """Initialize the configuration loader."""
        self.environment = environment
        self.sources = list(sources)
        self._writable_dict = {}
        if not deferred:
            self._config = self._load()

    def _load(self) -> t.MutableMapping[str, t.Any]:
        """Load configuration from sources."""
        scopes = _ConfigScopes(
            explicit=_to_box({}), environment=_to_box({}), baseline=_to_box({})
        )
        for source in self.sources:
            if isinstance(source, dict):
                # User may provide configuration as a dictionary directly
                # in which case it takes precedence over other sources
                scopes.explicit.merge_update(source)
            elif isinstance(source, (str, Path)):
                # Load configuration from file
                path = Path(source)
                result = load_file(path)
                if result.is_ok():
                    scopes.baseline.merge_update(result.unwrap())
                else:
                    err = result.unwrap_err()
                    if not isinstance(err, FileNotFoundError):
                        logger.warning(
                            f"Failed to load configuration from {path}: {result.unwrap_err()}"
                        )
                    else:
                        logger.debug(f"Configuration file not found: {path}")
                # Load environment-specific configuration from corresponding file
                # e.g. config.dev.json, config.dev.yaml, config.dev.toml
                env_path = path.with_name(
                    f"{path.stem}.{self.environment}{path.suffix}"
                )
                result = load_file(env_path)
                if result.is_ok():
                    scopes.environment.merge_update(result.unwrap())
                else:
                    err = result.unwrap_err()
                    if not isinstance(err, FileNotFoundError):
                        logger.warning(
                            f"Failed to load configuration from {path}: {err}"
                        )
                    else:
                        logger.debug(f"Configuration file not found: {env_path}")
        return ChainMap(self._writable_dict, scopes.resolve())

    @property
    def config(self) -> t.Mapping[str, t.Any]:
        """Get the configuration dictionary."""
        if not hasattr(self, "_config"):
            self._config = self._load()
        return self._config

    def import_(self, source: ConfigSource, append: bool = True) -> None:
        """Include a new source of configuration."""
        if append:
            # Takes priority within the same scope
            self.sources.append(source)
        else:
            self.sources.insert(0, source)
        self._config = self._load()


_MISSING: t.Any = object()
"""A sentinel value for a missing configuration value."""


def map_section(*sections: str) -> t.Callable[[t.Callable[P, T]], t.Callable[P, T]]:
    """Mark a function to inject configuration values from a specific section."""

    def decorator(func: t.Callable[P, T]) -> t.Callable[P, T]:
        setattr(func, "_sections", sections)
        return func

    return decorator


def map_values(**mapping: t.Any) -> t.Callable[[t.Callable[P, T]], t.Callable[P, T]]:
    """Mark a function to inject configuration values from a specific mapping of param names to keys."""

    def decorator(func: t.Callable[P, T]) -> t.Callable[P, T]:
        setattr(func, "_lookups", mapping)
        return func

    return decorator


class ConfigResolver(t.MutableMapping):
    """Resolve configuration values."""

    map_section = staticmethod(map_section)
    """Mark a function to inject configuration values from a specific section."""
    map_values = staticmethod(map_values)
    """Mark a function to inject configuration values from a specific mapping of param names to keys."""

    def __init__(
        self,
        *sources: ConfigSource,
        environment: str = "dev",
        loader: ConfigLoader = ConfigLoader("config.json"),
    ) -> None:
        """Initialize the configuration resolver."""
        for source in sources:
            loader.import_(source)
        self._loader = loader
        self._frozen_environment = os.environ.copy()

    @property
    def config(self) -> t.Mapping[str, t.Any]:
        """Get the configuration dictionary."""
        return self._loader.config

    def __getitem__(self, key: str) -> t.Any:
        """Get a configuration value."""
        v = self.config[key]
        return self.apply_converters(v, **self.config)

    def __setitem__(self, key: str, value: t.Any) -> None:
        """Set a configuration value."""
        self._loader._writable_dict[key] = value

    def __delitem__(self, key: str) -> None:
        self._loader._writable_dict.pop(key, None)

    def __iter__(self) -> t.Iterator[str]:
        return iter(self.config)

    def __len__(self) -> int:
        return len(self.config)

    def __getattr__(self, key: str) -> t.Any:
        """Get a configuration value."""
        try:
            return self[key]
        except KeyError as e:
            raise AttributeError from e

    def __enter__(self) -> "ConfigResolver":
        """Enter a context."""
        return self

    def __exit__(self, *args) -> None:
        """Exit a context."""
        os.environ.clear()
        os.environ.update(self._frozen_environment)

    def __repr__(self) -> str:
        """Get a string representation of the configuration resolver."""
        return f"{self.__class__.__name__}(<{len(self._loader.sources)} sources>)"

    def set_environment(self, environment: str) -> None:
        """Set the environment of the configuration resolver."""
        self._loader.environment = environment

    def import_(self, source: ConfigSource, append: bool = True) -> None:
        """Include a new source of configuration."""
        self._loader.import_(source, append)

    add_custom_converter = staticmethod(add_custom_converter)
    apply_converters = staticmethod(apply_converters)

    def inject_defaults(self, func: t.Callable[P, T]) -> t.Callable[..., T]:
        """Inject configuration values into a function."""
        sig = inspect.signature(func)

        sections = getattr(func, "_sections", ())
        explicit_lookups = getattr(func, "_lookups", {})

        @functools.wraps(func)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
            bound = sig.bind_partial(*args, **kwargs)
            for name, param in sig.parameters.items():
                if param.default not in (param.empty, None):
                    continue
                lookup = explicit_lookups.get(name, ".".join((*sections, name)))
                value = self.get(lookup, _MISSING)
                if value is not _MISSING:
                    bound.arguments[name] = self.apply_converters(value, **self.config)
            return func(*bound.args, **bound.kwargs)

        return wrapper

    def __call__(
        self, func_or_cls: t.Callable[P, T], *args: t.Any, **kwargs: t.Any
    ) -> T:
        """Invoke a callable with injected configuration values."""
        return self.inject_defaults(func_or_cls)(*args, **kwargs)


__all__ = [
    "ConfigLoader",
    "ConfigResolver",
    "ConfigSource",
    "add_custom_converter",
    "remove_converter",
]
