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


def _to_yaml(obj):
    buf = io.StringIO()
    YAML.dump(obj, buf)
    return buf.getvalue()


def _from_yaml(s):
    buf = io.StringIO(s)
    return YAML.load(buf)


class ShortEnvExtension(jinja2.ext.Extension):
    """
    Jinja2 extension to add a filter for environment variables.

    Usage: {{ "HOME"|env }}
    """

    def __init__(self, environment):
        super().__init__(environment)
        environment.filters["env"] = lambda s: os.getenv(s)
        environment.filters["envq"] = lambda s: f'"{os.getenv(s)}"'
        environment.filters["to_json"] = lambda s: json.dumps(s)
        environment.filters["from_json"] = lambda s: json.loads(s)
        environment.filters["to_yaml"] = _to_yaml
        environment.filters["from_yaml"] = _from_yaml
        environment.filters["to_toml"] = lambda s: tomlkit.dumps(s)
        environment.filters["from_toml"] = lambda s: tomlkit.loads(s)


ENVIRONMENT = jinja2.Environment(
    extensions=["jinja2.ext.do", "jinja2.ext.loopcontrols", ShortEnvExtension],
)


JINJA_METHODS = {
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
}
"""
Methods available to the config rendering context.

workspace() and workspace_root() are added dynamically.
"""
