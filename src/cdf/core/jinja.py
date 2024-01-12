import ast
import base64
import getpass
import io
import json
import os
import typing as t
from datetime import datetime, timedelta

import jinja2
import jinja2.ext
import ruamel.yaml
import tomlkit

import cdf.core.context as context

YAML = ruamel.yaml.YAML(typ="safe")
NOESCAPE = "cdf.noescape"


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


def _cat(*paths: str) -> str:
    """Reads one or more files into a string."""
    buf = bytearray()
    for path in paths:
        with open(path, "rb") as f:
            buf.extend(f.read())
            if not buf[-1] == b"\n":
                buf.extend(b"\n")
    return buf.decode("utf-8")


def _head(input_: str, n: int = 10) -> str:
    """Returns the first n lines of a string."""
    return "\n".join(input_.split("\n")[:n])


def _include(path: str, **kwargs: t.Any) -> str:
    """Includes a TOML file in the template"""
    return NOESCAPE + render(_cat(path), **kwargs)


def _noescape(s: str) -> str:
    """Prevents a string from being escaped."""
    return NOESCAPE + s


class CDFExtension(jinja2.ext.Extension):
    """A Jinja2 extension for CDF."""

    def __init__(self, environment):
        super().__init__(environment)
        environment.filters["tojson"] = lambda s: json.dumps(s)
        environment.filters["fromjson"] = lambda s: json.loads(s)
        environment.filters["toyaml"] = _to_yaml
        environment.filters["fromyaml"] = _from_yaml
        environment.filters["totoml"] = lambda s: NOESCAPE + tomlkit.dumps(s)
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
        environment.filters["raw"] = _noescape


def _finalize(s: T) -> str:
    """Converts native types to TOML, escaping linefeeds."""
    if isinstance(s, dict):
        return tomlkit.dumps(_recursive_escape_linefeeds(s))
    if isinstance(s, list):
        return tomlkit.dumps({"_": _recursive_escape_linefeeds(s)})[4:-1]
    if isinstance(s, str):
        if s.startswith(NOESCAPE):
            step = len(NOESCAPE)
            while s.startswith(NOESCAPE):
                s = s[step:]
            return s
    return _escape_linefeeds(str(s))


ENVIRONMENT = jinja2.Environment(
    extensions=["jinja2.ext.do", "jinja2.ext.loopcontrols", CDFExtension],
    finalize=_finalize,
    keep_trailing_newline=True,
)
"""The Jinja2 environment for CDF."""


BASE_JINJA_CONTEXT = {
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
    "include": _include,
    "getuser": getpass.getuser,
    "workspace": None,
    "root": None,
}
"""
Methods available to the config rendering context.

workspace and root are injected dynamically.
"""


def render(
    template: str,
    **kwargs,
) -> str:
    """
    Renders a template with CDF semantics.

    The philosophy of CDF configuration is that it should be as simple as possible. We do not want to construct correct
    TOML documents character by character using complex string interpolation and concatenation. When native types
    such as dictionaries and lists are used, we want them to be rendered as TOML tables and arrays by default
    since that is our medium. We want to escape linefeeds in strings so that they do not break our TOML documents.
    We expect strings to hold values, not structure. We support looping and conditionals and other niceties to
    keep our configuration DRY. We support a custom parametrized `include` function to keep our configuration modular.

    Templates are rendered with Jinja2, with the following additional filters:
    - tojson: Converts a value to JSON.
    - fromjson: Converts a value from JSON.
    - toyaml: Converts a value to YAML.
    - fromyaml: Converts a value from YAML.
    - totoml: Converts a value to TOML (with noescape).
    - fromtoml: Converts a value from TOML.
    - env: Gets an environment variable. IE {{ "SOME_VAR_" ~ j | env }}
    - b64encode: Base64 encodes a value.
    - b64decode: Base64 decodes a value.
    - head: Returns the first n lines of a string.
    - eval: Evaluates a string as a Python expression.

    The following additional functions are available:
    - today: Returns today's date in YYYY-MM-DD format.
    - yesterday: Returns yesterday's date in YYYY-MM-DD format.
    - tomorrow: Returns tomorrow's date in YYYY-MM-DD format.
    - now: Returns the current datetime in YYYY-MM-DD HH:MM:SS format.
    - now_utc: Returns the current datetime in YYYY-MM-DD HH:MM:SS format in UTC.
    - days_ago: Returns the date n days ago in YYYY-MM-DD format.
    - days_later: Returns the date n days later in YYYY-MM-DD format.
    - weeks_ago: Returns the date n weeks ago in YYYY-MM-DD format.
    - weeks_later: Returns the date n weeks later in YYYY-MM-DD format.
    - cat: Concatenates one or more files into a string.
    - getuser: Gets the current user.
    - workspace: The active workspace.
    - root: The root of the active workspace.
    - env_var: Gets an environment variable with a default value.
    - include: Includes a cdf TOML file in the template.

    Args:
        template: The template to render.
        **kwargs: Additional kwargs to pass to the template.

    Returns:
        str: The rendered template.
    """
    # Priority: kwargs > env > context
    ctx = BASE_JINJA_CONTEXT.copy()
    ctx.update(os.environ)
    ctx.update(kwargs)

    # Inject workspace and root if active
    if workspace := context.get_active_workspace():
        ctx["workspace"] = workspace
        ctx["root"] = workspace.root

    return ENVIRONMENT.from_string(template).render(ctx)
