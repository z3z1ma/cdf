# pyright: reportUnknownMemberType=false
"""Configuration loader for CDF."""

from __future__ import annotations

import ast
import collections
import datetime
import os
import re
import string
import typing as t
from collections.abc import Iterable, Mapping, MutableMapping, ValuesView
from pathlib import Path

import dateutil.parser
from box import Box

from cdf.utils.files import json, load_file_from_extension

__all__ = [
    "ConfigurationSource",
    "ConfigBox",
    "ConfigurationLoader",
    "add_custom_converter",
    "apply_converters",
    "get_converter",
    "remove_converter",
]

ConfigurationSource = str | Path | Mapping[str, t.Any] | t.Callable[[], "ConfigurationSource"]


def _to_bool(value: str) -> bool:
    """Convert a string to a boolean value."""
    return value.lower() in ("true", "yes", "1")


def _to_datetime(value: str) -> datetime.datetime:
    """Convert a string to a datetime object."""
    return dateutil.parser.parse(value)


def _to_date(value: str) -> datetime.date:
    """Convert a string to a date object."""
    return dateutil.parser.parse(value).date()


def _make_eval_func(type_: type):
    """Create a function to evaluate a py literal string with type assertion."""

    def _eval(value: str) -> t.Any:
        v = ast.literal_eval(value)
        if not isinstance(v, type_):
            raise ValueError(f"Value is not of type {type_}")
        return v

    return _eval


_CONVERTERS: dict[str, t.Callable[[str], t.Any]] = {
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
    "datetime": _to_datetime,
    "date": _to_date,
}
"""Converters for configuration values."""

_CONVERTER_PATTERN = re.compile(r"@(\w+) ", re.IGNORECASE)
"""Pattern to match converters in a string."""


def add_custom_converter(name: str, converter: t.Callable[[str], t.Any]) -> None:
    """Add a custom converter to the configuration system."""
    if name.lower() in _CONVERTERS:
        raise ValueError(f"Converter {name} already exists.")
    _CONVERTERS[name.lower()] = converter


def get_converter(name: str) -> t.Callable[[str], t.Any]:
    """Get a custom converter from the configuration system."""
    return _CONVERTERS[name.lower()]


def remove_converter(name: str) -> None:
    """Remove a custom converter from the configuration system."""
    if name.lower() not in _CONVERTERS:
        raise ValueError(f"Converter {name} does not exist.")
    del _CONVERTERS[name.lower()]


def apply_converters(data: t.Any, /, partial_conf: ConfigBox | None = None) -> t.Any:
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
    - resolve: A meta converter to resolve value from partial configuration

    Args:
        data: Configuration value to apply converters to.

    Raises:
        ValueError: If an unknown converter is used or if a conversion fails.

    Returns:
        Converted configuration value.
    """
    if not isinstance(data, str):
        return data
    data = string.Template(data).safe_substitute(os.environ)
    converters = _CONVERTER_PATTERN.findall(data)
    if len(converters) == 0:
        return data
    base_v = _CONVERTER_PATTERN.sub("", data).lstrip()
    if not base_v:
        return None
    transformed_v = base_v
    for converter in reversed(converters):
        try:
            if converter.lower() == ConfigBox.META_CONVERTER:
                if partial_conf is None:
                    raise ValueError("Partial configuration not provided for resolver")
                try:
                    transformed_v = partial_conf[transformed_v]
                except KeyError as e:
                    raise ValueError(f"Key not found in resolver: {e}") from e
            else:
                transformed_v = get_converter(converter)(transformed_v)
        except KeyError as e:
            raise ValueError(f"Unknown converter: {converter}") from e
        except Exception as e:
            raise ValueError(f"Failed to convert value: {e}") from e
    return transformed_v


class ConfigBox(Box):
    """Box that applies @ converters to configuration values."""

    META_CONVERTER: t.ClassVar[str] = "resolve"

    def __getitem__(self, item: t.Any, _ignore_default: bool = False) -> t.Any:
        value = t.cast(t.Any, super().__getitem__(item, _ignore_default))
        if isinstance(value, str):
            return self._apply_converters(value)
        return value

    def values(self) -> ValuesView[t.Any]:  # pyright: ignore[reportIncompatibleMethodOverride]
        dict_self = t.cast(dict[str, t.Any], self)
        for k, v in dict_self.items():
            self[k] = self._apply_converters(v)
        return ValuesView(dict_self)

    def _apply_converters(self, data: t.Any, /) -> t.Any:
        """Apply converters to a configuration value."""
        return apply_converters(data, self)

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(...)"


def _merge_configs(*configs: MutableMapping[str, t.Any]) -> ConfigBox:
    """Combine multiple configuration Boxes using merge_update."""
    merged = ConfigBox()
    for config in configs:
        merged.merge_update(config)
    return merged


def _scope_configs(*configs: MutableMapping[str, t.Any]) -> ConfigBox:
    """Combine multiple configuration Boxes via ChainMap to provide scope-based resolution."""
    return ConfigBox(collections.ChainMap(*configs))


class ConfigurationLoader:
    """Loads configuration from multiple sources and merges them using a resolution strategy."""

    SUPPORTED_EXTENSIONS: t.ClassVar[tuple[str, ...]] = ("json", "yaml", "yml", "toml", "py")

    def __init__(
        self,
        *sources: ConfigurationSource,
        resolution_strategy: t.Literal["merge", "scope"] = "merge",
        include_envvars: bool = True,
    ) -> None:
        """Initialize the configuration loader with given sources.

        Args:
            sources: Configuration sources to load.
            resolution_strategy: Strategy to use for merging configurations
            - "merge": Merge all configurations into a single Box
            - "scope": Combine configurations into a ChainMap for scope-based resolution
            include_envvars: Whether to include environment variables

        Raises:
            ValueError: If an unsupported resolution strategy is provided
        """
        self.sources: tuple[ConfigurationSource, ...] = sources
        if include_envvars:
            self.sources += (dict(os.environ),)
        self._config: ConfigBox | None = None
        self._resolution_strategy: str = resolution_strategy
        self._resolver: t.Callable[..., ConfigBox] = (
            _merge_configs if resolution_strategy == "merge" else _scope_configs
        )

    @classmethod
    def from_name(
        cls, name: str, /, *, search_paths: Iterable[Path] | None = None
    ) -> ConfigurationLoader:
        """Create a configuration loader from a name by searching for files with supported extensions.

        Args:
            name: Name of the configuration file.
            search_paths: Paths to search for the named configuration file.

        Returns:
            Configuration loader with the found configuration files.
        """
        return cls(
            *tuple(
                conf_path
                for ext in cls.SUPPORTED_EXTENSIONS
                for path in search_paths or [Path.cwd()]
                for conf_path in path.glob(f"{name}.{ext}")
            )
        )

    def add_source(self, source: ConfigurationSource) -> Mapping[str, t.Any]:
        """Add a configuration source to the loader.

        Args:
            source: Configuration source to add.
        """
        self.sources += (source,)
        return self.load()

    def load(self) -> ConfigBox:
        """Load and merge configurations from all sources."""
        configs = [Box(self._load(source)) for source in self.sources]
        self._config = self._resolver(
            *(configs if self._resolution_strategy == "merge" else reversed(configs))
        )
        return self._config

    @staticmethod
    def _load(source: ConfigurationSource) -> Mapping[str, t.Any]:
        """Load configuration from a single source.

        Args:
            source: Configuration source to load.

        Returns:
            Configuration as a dictionary.
        """
        if callable(source):
            return ConfigurationLoader._load(source())
        elif isinstance(source, dict):
            return source
        elif isinstance(source, (str, Path)):
            return load_file_from_extension(source)
        else:
            raise TypeError(f"Invalid config source: {source}")
