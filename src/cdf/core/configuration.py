"""Configuration utilities for the CDF configuration resolver system.

There are 3 ways to request configuration values:

1. Using a Request annotation:

Pro: It's explicit and re-usable. An annotation can be used in multiple places.

```python
import typing as t
import cdf.core.configuration as conf

def foo(bar: t.Annotated[str, conf.Request["api.key"]]) -> None:
    print(bar)
```

2. Setting a __cdf_resolve__ attribute on a callable object. This can be done
directly or by using the `map_section` or `map_values` decorators:

Pro: It's concise and can be used in a decorator. It also works with classes.

```python
import cdf.core.configuration as conf

@conf.map_section("api")
def foo(key: str) -> None:
    print(key)

@conf.map_values(key="api.key")
def bar(key: str) -> None:
    print(key)

def baz(key: str) -> None:
    print(key)

baz.__cdf_resolve__ = ("api",)
```

3. Using the `_cdf_resolve` kwarg to request the resolver:

Pro: It's flexible and can be used in any function. It requires no imports.

```python
def foo(key: str, _cdf_resolve=("api",)) -> None:
    print(key)

def bar(key: str, _cdf_resolve={"key": "api.key"}) -> None:
    print(key)
```
"""

import ast
import functools
import inspect
import json
import logging
import os
import re
import string
import typing as t
from collections import ChainMap
from contextlib import suppress
from pathlib import Path

import pydantic
import pydantic_core
from typing_extensions import ParamSpec

if t.TYPE_CHECKING:
    from dynaconf.vendor.box import Box

from cdf.types import M

logger = logging.getLogger(__name__)

T = t.TypeVar("T")
P = ParamSpec("P")

__all__ = [
    "ConfigLoader",
    "ConfigResolver",
    "ConfigSource",
    "Request",
    "add_custom_converter",
    "remove_converter",
    "load_file",
    "map_config_section",
    "map_config_values",
]


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
        import ruamel.yaml as yaml

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
        import tomlkit

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


def get_converter(name: str) -> t.Callable[[str], t.Any]:
    """Get a custom converter from the configuration system."""
    return _CONVERTERS[name]


def remove_converter(name: str) -> None:
    """Remove a custom converter from the configuration system."""
    if name not in _CONVERTERS:
        raise ValueError(f"Converter {name} does not exist.")
    del _CONVERTERS[name]


def apply_converters(
    input_value: t.Any, resolver: t.Optional["ConfigResolver"] = None
) -> t.Any:
    """Apply converters to a string."""
    if not isinstance(input_value, str):
        return input_value
    expanded_value = _resolve_template(input_value)
    converters = _CONVERTER_PATTERN.findall(expanded_value)
    if len(converters) == 0:
        return expanded_value
    base_value = _CONVERTER_PATTERN.sub("", expanded_value).lstrip()
    if not base_value:
        return None
    transformed_value = base_value
    for converter in reversed(converters):
        try:
            if converter.lower() == "resolve":
                if resolver is None:
                    raise ValueError(
                        "Resolver instance not provided but found @resolve converter"
                    )
                if transformed_value not in resolver:
                    raise ValueError(f"Key not found in resolver: {transformed_value}")
                transformed_value = resolver[transformed_value]
                continue
            transformed_value = _CONVERTERS[converter.lower()](transformed_value)
        except KeyError as e:
            raise ValueError(f"Unknown converter: {converter}") from e
        except Exception as e:
            raise ValueError(f"Failed to convert value: {e}") from e
    return transformed_value


def _to_box(mapping: t.Mapping[str, t.Any]) -> "Box":
    """Convert a mapping to a standardized Box."""
    from dynaconf.vendor.box import Box

    return Box(mapping, box_dots=True)


class _ConfigScopes(t.NamedTuple):
    """A struct to store named configuration scopes by precedence."""

    explicit: "Box"
    """User-provided configuration passed as a dictionary."""
    environment: "Box"
    """Environment-specific configuration loaded from a file."""
    baseline: "Box"
    """Configuration loaded from a base config file."""

    def resolve(self) -> "Box":
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
    ) -> None:
        """Initialize the configuration loader."""
        self.environment = environment
        self.sources = list(sources)

    def load(self) -> t.MutableMapping[str, t.Any]:
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
        return scopes.resolve()

    def import_source(self, source: ConfigSource, append: bool = True) -> None:
        """Include a new source of configuration."""
        if append:
            # Takes priority within the same scope
            self.sources.append(source)
        else:
            self.sources.insert(0, source)

    def clear_sources(self) -> t.List[ConfigSource]:
        """Clear all sources of configuration returning the previous sources."""
        cleared_sources = self.sources.copy()
        self.sources.clear()
        return cleared_sources


_MISSING: t.Any = object()
"""A sentinel value for a missing configuration value."""

RESOLVER_HINT = "__cdf_resolve__"
"""A hint to engage the configuration resolver."""


def map_config_section(
    *sections: str,
) -> t.Callable[[t.Callable[P, T]], t.Callable[P, T]]:
    """Mark a function to inject configuration values from a specific section."""

    def decorator(func_or_cls: t.Callable[P, T]) -> t.Callable[P, T]:
        setattr(inspect.unwrap(func_or_cls), RESOLVER_HINT, sections)
        return func_or_cls

    return decorator


def map_config_values(
    **mapping: t.Any,
) -> t.Callable[[t.Callable[P, T]], t.Callable[P, T]]:
    """Mark a function to inject configuration values from a specific mapping of param names to keys."""

    def decorator(func_or_cls: t.Callable[P, T]) -> t.Callable[P, T]:
        setattr(inspect.unwrap(func_or_cls), RESOLVER_HINT, mapping)
        return func_or_cls

    return decorator


class Request:
    def __init__(self, item: str):
        self.item = item

    def __class_getitem__(cls, item: str) -> "Request":
        return cls(item)


class ConfigLoaderProtocol(t.Protocol):
    environment: str
    sources: t.List[ConfigSource]

    def load(self) -> t.MutableMapping[str, t.Any]: ...

    def import_source(self, source: ConfigSource, append: bool = True) -> None: ...

    def clear_sources(self) -> t.List[ConfigSource]: ...


class ConfigResolver(t.MutableMapping[str, t.Any]):
    """Resolve configuration values."""

    def __init__(
        self,
        *sources: ConfigSource,
        environment: str = "dev",
        loader: ConfigLoaderProtocol = ConfigLoader("config.json"),
        deferred: bool = False,
    ) -> None:
        """Initialize the configuration resolver.

        The environment serves 2 purposes:
        1. It determines supplementary configuration file to load, e.g. config.dev.json.
        2. It prefixes configuration keys and prioritizes them over non-prefixed keys. e.g. dev.api.key.

        These are not mutually exclusive and can be used together.

        Args:
            sources: The sources of configuration.
            environment: The environment to load configuration for.
            loader: The configuration loader.
            deferred: If True, the configuration is not loaded until requested.
        """
        self.environment = environment
        for source in sources:
            loader.import_source(source)
        self._loader = loader
        self._config = loader.load() if not deferred else None
        self._frozen_environment = os.environ.copy()
        self._explicit_values = _to_box({})

    @property
    def wrapped(self) -> t.MutableMapping[str, t.Any]:
        """Get the configuration dictionary."""
        if self._config is None:
            self._config = _to_box(self._loader.load())
        return ChainMap(self._explicit_values, self._config)

    def __getitem__(self, key: str) -> t.Any:
        """Get a configuration value."""
        try:
            v = self.wrapped[f"{self.environment}.{key}"]
        except KeyError:
            v = self.wrapped[key]
        return self.apply_converters(v, self)

    def __setitem__(self, key: str, value: t.Any) -> None:
        """Set a configuration value."""
        self._explicit_values[f"{self.environment}.{key}"] = value

    def __delitem__(self, key: str) -> None:
        self._explicit_values.pop(f"{self.environment}.{key}", None)

    def __iter__(self) -> t.Iterator[str]:
        return iter(self.wrapped)

    def __len__(self) -> int:
        return len(self.wrapped)

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
        self.environment = environment
        self._loader.environment = environment
        self._config = None

    def import_source(self, source: ConfigSource, append: bool = True) -> None:
        """Include a new source of configuration."""
        self._loader.import_source(source, append)
        self._config = None

    def clear_sources(self) -> t.List[ConfigSource]:
        """Clear all sources of configuration returning the previous sources."""
        sources = self._loader.clear_sources()
        self._config = None
        return sources

    map_section = staticmethod(map_config_section)
    """Mark a function to inject configuration values from a specific section."""

    map_values = staticmethod(map_config_values)
    """Mark a function to inject configuration values from a specific mapping of param names to keys."""

    add_custom_converter = staticmethod(add_custom_converter)
    """Add a custom converter to the configuration system."""

    apply_converters = staticmethod(apply_converters)
    """Apply converters to a string."""

    KWARG_HINT = "_cdf_resolve"
    """A hint supplied in a kwarg to engage the configuration resolver."""

    def _parse_hint_from_params(
        self, func_or_cls: t.Callable, sig: t.Optional[inspect.Signature] = None
    ) -> t.Optional[t.Union[t.Tuple[str, ...], t.Mapping[str, str]]]:
        """Get the sections or explicit lookups from a function.

        This assumes a kwarg named `_cdf_resolve` that is either a tuple of section names or
        a dictionary of param names to config keys is present in the function signature.
        """
        sig = sig or inspect.signature(func_or_cls)
        if self.KWARG_HINT in sig.parameters:
            resolver_spec = sig.parameters[self.KWARG_HINT]
            if isinstance(resolver_spec.default, (tuple, dict)):
                return resolver_spec.default

    def resolve_defaults(self, func_or_cls: t.Callable[P, T]) -> t.Callable[..., T]:
        """Resolve configuration values into a function or class."""
        if not callable(func_or_cls):
            return func_or_cls

        sig = inspect.signature(func_or_cls)
        is_resolved_sentinel = "__config_resolved__"

        resolver_hint = getattr(
            inspect.unwrap(func_or_cls),
            RESOLVER_HINT,
            self._parse_hint_from_params(func_or_cls, sig),
        )

        if any(hasattr(f, is_resolved_sentinel) for f in _iter_wrapped(func_or_cls)):
            return func_or_cls

        @functools.wraps(func_or_cls)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
            bound_args = sig.bind_partial(*args, **kwargs)
            bound_args.apply_defaults()

            # Apply converters to string literal arguments
            for arg_name, arg_value in bound_args.arguments.items():
                if isinstance(arg_value, str):
                    with suppress(Exception):
                        bound_args.arguments[arg_name] = self.apply_converters(
                            arg_value,
                            self,
                        )

            # Resolve configuration values
            for name, param in sig.parameters.items():
                value = _MISSING
                if not self.is_resolvable(param):
                    continue

                # 1. Prioritize Request annotations
                elif request := self.extract_request_annotation(param):
                    value = self.get(request, _MISSING)

                # 2. Use explicit lookups if provided
                elif isinstance(resolver_hint, dict):
                    if name not in resolver_hint:
                        continue
                    value = self.get(resolver_hint[name], _MISSING)

                # 3. Use section-based lookups if provided
                elif isinstance(resolver_hint, (tuple, list)):
                    value = self.get(".".join((*resolver_hint, name)), _MISSING)

                # Inject the value into the function
                if value is not _MISSING:
                    bound_args.arguments[name] = self.apply_converters(value, self)

            return func_or_cls(*bound_args.args, **bound_args.kwargs)

        setattr(wrapper, is_resolved_sentinel, True)
        return wrapper

    def is_resolvable(self, param: inspect.Parameter) -> bool:
        """Check if a parameter is injectable."""
        return param.default in (param.empty, None)

    @staticmethod
    def extract_request_annotation(param: inspect.Parameter) -> t.Optional[str]:
        """Extract a request annotation from a parameter."""
        for hint in getattr(param.annotation, "__metadata__", ()):
            if isinstance(hint, Request):
                return hint.item

    def __call__(
        self, func_or_cls: t.Callable[P, T], *args: t.Any, **kwargs: t.Any
    ) -> T:
        """Invoke a callable with injected configuration values."""
        configured_f = self.resolve_defaults(func_or_cls)
        if not callable(configured_f):
            return configured_f
        return configured_f(*args, **kwargs)

    @classmethod
    def __get_pydantic_core_schema__(
        cls, source_type: t.Any, handler: pydantic.GetCoreSchemaHandler
    ) -> pydantic_core.CoreSchema:
        return pydantic_core.core_schema.dict_schema(
            keys_schema=pydantic_core.core_schema.str_schema(),
            values_schema=pydantic_core.core_schema.any_schema(),
        )


def _iter_wrapped(f: t.Callable):
    yield f
    f_w = inspect.unwrap(f)
    if f_w is not f:
        yield from _iter_wrapped(f_w)
