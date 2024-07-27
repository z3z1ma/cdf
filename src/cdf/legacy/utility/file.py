import json
import typing as t
from pathlib import Path

import ruamel.yaml as yaml
import tomlkit

from cdf.types import M


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
        return M.ok(yaml.round_trip_load(path, preserve_quotes=True))
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
