import ast
import base64
import io
import json
import os
import typing as t
from datetime import datetime, timedelta

import jinja2
import jinja2.ext
import ruamel.yaml
import tomlkit

YAML = ruamel.yaml.YAML(typ="safe")

T = t.TypeVar("T", dict, list, str)


def _to_yaml(obj: T) -> str:
    """Dumps a dictionary to a YAML string."""
    buf = io.StringIO()
    YAML.dump(obj, buf)
    return buf.getvalue()


def _from_yaml(s: str) -> T:
    """Loads a YAML string into a dictionary."""
    buf = io.StringIO(s)
    return YAML.load(buf)


def _escape_linefeeds(s: str) -> str:
    """Escapes linefeeds in a string."""
    return s.replace("\n", "\\n")


def _recursive_escape_linefeeds(obj: T) -> T:
    """Recursively escape linefeeds in a dictionary or string."""
    if isinstance(obj, dict):
        return {k: _recursive_escape_linefeeds(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [_recursive_escape_linefeeds(v) for v in obj]
    elif isinstance(obj, str):
        return _escape_linefeeds(obj)
    return obj


def _cat(path: str) -> str:
    """Reads a file into a string."""

    with open(path, "r", encoding="utf-8") as f:
        return f.read()


def _head(input: str, n: int = 10) -> str:
    """Returns the first n lines of a string."""
    return "\n".join(input.split("\n")[:n])


class CDFExtension(jinja2.ext.Extension):
    def __init__(self, environment):
        super().__init__(environment)
        environment.filters["tojson"] = lambda s: json.dumps(s)
        environment.filters["fromjson"] = lambda s: json.loads(s)
        environment.filters["toyaml"] = _to_yaml
        environment.filters["fromyaml"] = _from_yaml
        environment.filters["totoml"] = lambda s: tomlkit.dumps(s)
        environment.filters["fromtoml"] = lambda s: tomlkit.loads(s)
        environment.filters["env"] = lambda s, default=None: os.getenv(s, default)
        environment.filters["b64encode"] = lambda s: base64.b64encode(
            s.encode("utf-8")
        ).decode("utf-8")
        environment.filters["b64decode"] = lambda s: base64.b64decode(
            s.encode("utf-8")
        ).decode("utf-8")
        environment.filters["head"] = _head
        environment.filters["eval"] = lambda s: ast.literal_eval(s)


def _finalize(s: T) -> str:
    """Converts native types to TOML, escaping linefeeds."""
    v = _recursive_escape_linefeeds(s)
    if isinstance(v, dict):
        return tomlkit.dumps(v)
    if isinstance(v, list):
        return tomlkit.dumps({"_": v})[4:-1]
    return v


ENVIRONMENT = jinja2.Environment(
    extensions=["jinja2.ext.do", "jinja2.ext.loopcontrols", CDFExtension],
    finalize=_finalize,
    keep_trailing_newline=True,
)


JINJA_CONTEXT = {
    "env_var": lambda key, default=None: os.getenv(key, default),
    "today": lambda: datetime.now().strftime("%Y-%m-%d"),
    "yesterday": lambda: (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d"),
    "tomorrow": lambda: (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d"),
    "now": lambda: datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    "now_utc": lambda: datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
    "days_ago": lambda n: (datetime.now() - timedelta(days=n)).strftime("%Y-%m-%d"),
    "days_later": lambda n: (datetime.now() + timedelta(days=n)).strftime("%Y-%m-%d"),
    "weeks_ago": lambda n: (datetime.now() - timedelta(weeks=n)).strftime("%Y-%m-%d"),
    "weeks_later": lambda n: (datetime.now() + timedelta(weeks=n)).strftime("%Y-%m-%d"),
    "cat": _cat,
    "workspace": None,
    "root": None,
}
"""
Methods available to the config rendering context.

workspace and root are injected dynamically.
"""
