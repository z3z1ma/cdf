import base64
import io
import json
import os
from datetime import datetime, timedelta

import jinja2
import jinja2.ext
import ruamel.yaml
import tomlkit

YAML = ruamel.yaml.YAML(typ="safe")


def _to_yaml(obj: dict) -> str:
    buf = io.StringIO()
    YAML.dump(obj, buf)
    return buf.getvalue()


def _from_yaml(s: str) -> dict:
    buf = io.StringIO(s)
    return YAML.load(buf)


def _escape_linefeeds(s: str) -> str:
    return s.replace("\n", "\\n")


def _recursive_escape_linefeeds(obj: dict | list) -> dict | list:
    if isinstance(obj, dict):
        return {k: _recursive_escape_linefeeds(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [_recursive_escape_linefeeds(v) for v in obj]
    elif isinstance(obj, str):
        return _escape_linefeeds(obj)
    return obj


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


def _finalize(s: str) -> str:
    if isinstance(s, (dict, list)):
        return _to_toml(_recursive_escape_linefeeds(s))
    elif isinstance(s, str):
        return _escape_linefeeds(s)
    return s


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
    "b64encode": lambda s: base64.b64encode(s.encode("utf-8")).decode("utf-8"),
    "b64decode": lambda s: base64.b64decode(s.encode("utf-8")).decode("utf-8"),
    "workspace": None,
    "root": None,
}
"""
Methods available to the config rendering context.

workspace and root are injected dynamically.
"""
