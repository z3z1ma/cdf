# pyright: reportUnreachable=false
"""File utilities for loading and parsing various file formats."""

import functools
import importlib
import importlib.util
import io
import json
import os
import string
import sys
import typing as t
from pathlib import Path
from types import ModuleType

import yaml

if sys.version_info >= (3, 11):
    import tomllib as toml
else:
    import tomli as toml

__all__ = [
    "toml",
    "yaml",
    "json",
    "load_file",
    "load_json",
    "load_yaml",
    "load_toml",
    "load_module_from_path",
    "load_file_from_extension",
    "clear_load_file_cache",
]


def _expand_vars(template: str, **context: t.Any) -> str:
    """Resolve variables in the format ${VAR} or $VAR from env and context.

    Args:
        template: The template string to resolve.
        **context: Additional context to use for variable expansion.

    Returns:
        The resolved string.
    """
    return string.Template(template).safe_substitute(os.environ, **context)


@functools.lru_cache(maxsize=128)
def load_file(
    path: Path | str,
    mode: str = "r",
    parser: t.Callable[[str], t.Any] = json.loads,
    expand_env_vars: bool = True,
    *,
    context: dict[str, t.Any] | None = None,
) -> t.Any:
    """Read a file from the given path and parse it using the specified parser.

    Args:
        path: Path to the file to read.
        mode: File open mode.
        parser: Parser function to use for the file content.
        expand_env_vars: Whether to expand environment variables in the file content.
        context: Additional context to use for variable expansion.

    Returns:
        The parsed file content.
    """
    with open(path, mode=mode) as f:
        raw = f.read()
        if expand_env_vars:
            context = context or {}
            rendered = _expand_vars(raw, **context)
        else:
            rendered = raw
    return parser(rendered)


clear_load_file_cache = load_file.cache_clear
"""Clear the file loading cache."""


def __yaml_safe_load(s: str) -> dict[str, t.Any]:
    return yaml.safe_load(io.StringIO(s))


load_json = functools.partial(load_file, parser=json.loads)
load_yaml = functools.partial(load_file, parser=__yaml_safe_load)
load_toml = functools.partial(load_file, parser=toml.loads)


def load_module_from_path(path: Path | str) -> ModuleType:
    """Load a Python module from a file path.

    Args:
        path: Path to the Python file to load.

    Returns:
        The loaded module's dictionary.
    """
    path = Path(path)
    spec = importlib.util.spec_from_file_location(path.stem, path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Could not load module from path: {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def load_file_from_extension(path: Path | str) -> dict[str, t.Any]:
    """Load file based on its extension.

    Args:
        path: Path to the file to load.

    Returns:
        The parsed file content.
    """
    path = Path(path)
    if not path.exists():
        return {}
    if path.suffix == ".json":
        return load_json(path)
    elif path.suffix in (".yaml", ".yml"):
        return load_yaml(path)
    elif path.suffix == ".toml":
        return load_toml(path)
    elif path.suffix == ".py":
        return load_module_from_path(path).__dict__["config"]
    else:
        raise ValueError(f"Unsupported file format: {path.suffix}")
