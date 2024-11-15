"""Configuration loader for CDF."""

import ast
import collections
import io
import json
import os
import re
import string
import sys
import typing as t
from pathlib import Path

import yaml
from box import Box

if sys.version_info >= (3, 11):
    import tomllib
else:
    import tomli as tomllib


ConfigurationSource = t.Union[
    str, Path, t.Mapping[str, t.Any], t.Callable[[], "ConfigurationSource"]
]


__all__ = [
    "ConfigurationSource",
    "ConfigBox",
    "ConfigurationLoader",
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
    return string.Template(template).safe_substitute(env_overrides, **os.environ)


def _load_file(
    path: t.Union[str, Path],
    mode: str = "r",
    parser: t.Callable[[str], t.Any] = json.loads,
    **env_overrides: t.Any,
) -> t.Any:
    """Read a file from the given path and parse it using the specified parser."""
    with open(path, mode=mode) as f:
        rendered = _expand_env_vars(f.read(), **env_overrides)
    return parser(rendered)


class ConfigBox(Box):
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


def _merge_configs(*configs: t.MutableMapping[str, t.Any]) -> ConfigBox:
    """Combine multiple configuration Boxes using merge_update."""
    merged = ConfigBox(box_dots=True)
    for config in configs:
        merged.merge_update(config)
    return merged


def _scope_configs(*configs: t.MutableMapping[str, t.Any]) -> ConfigBox:
    """Combine multiple configuration Boxes via ChainMap to provide scope-based resolution."""
    return ConfigBox(collections.ChainMap(*configs), box_dots=True)


class ConfigurationLoader:
    """Loads configuration from multiple sources and merges them using a resolution strategy."""

    SUPPORTED_EXTENSIONS = ("json", "yaml", "yml", "toml", "py")

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
            include_env: Whether to include environment variables

        Raises:
            ValueError: If an unsupported resolution strategy is provided
        """
        if resolution_strategy not in ("merge", "scope"):
            raise ValueError(f"Unsupported resolution strategy: {resolution_strategy}")
        self.sources = sources
        if include_envvars:
            self.sources += (dict(os.environ),)
        self._config = None
        self._resolution_strategy = resolution_strategy
        self._resolver = (
            _merge_configs if resolution_strategy == "merge" else _scope_configs
        )

    @classmethod
    def from_name(
        cls, name: str, /, *, search_path: t.Optional[Path] = None
    ) -> "ConfigurationLoader":
        """Create a configuration loader from a name by searching for files with supported extensions.

        Args:
            name: Name of the configuration file.
            search_path: Path to search for the configuration file.

        Returns:
            Configuration loader with the found configuration files.
        """
        path = search_path or Path.cwd()
        return cls(
            *tuple(
                conf_path
                for ext in cls.SUPPORTED_EXTENSIONS
                for conf_path in path.glob(f"{name}.{ext}")
            )
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
        self._config = self._resolver(
            *(configs if self._resolution_strategy == "merge" else reversed(configs))
        )
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
            return ConfigurationLoader._load(source())
        elif isinstance(source, dict):
            return source
        elif isinstance(source, (str, Path)):
            path = Path(source)
            if not path.exists():
                return {}
            if path.suffix == ".json":
                return _load_file(path, parser=json.loads)
            elif path.suffix in (".yaml", ".yml"):
                return _load_file(path, parser=lambda s: yaml.safe_load(io.StringIO(s)))
            elif path.suffix == ".toml":
                return _load_file(path, parser=tomllib.loads)
            else:
                raise ValueError(f"Unsupported file format: {path.suffix}")
        else:
            raise TypeError(f"Invalid config source: {source}")
