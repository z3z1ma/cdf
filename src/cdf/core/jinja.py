import base64
import os
from datetime import datetime, timedelta

import jinja2
import jinja2.ext


class ShortEnvExtension(jinja2.ext.Extension):
    """
    Jinja2 extension to add a filter for environment variables.

    Usage: {{ "HOME"|env }}
    """

    def __init__(self, environment):
        super().__init__(environment)
        environment.filters["env"] = lambda s: os.getenv(s)
        environment.filters["envq"] = lambda s: f'"{os.getenv(s)}"'


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
    "base64": lambda s: base64.b64encode(s.encode("utf-8")).decode("utf-8"),
    "base64_decode": lambda s: base64.b64decode(s.encode("utf-8")).decode("utf-8"),
}
"""
Methods available to the config rendering context.

workspace() and workspace_root() are added dynamically.
"""
