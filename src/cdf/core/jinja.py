import os
from datetime import datetime, timedelta

import jinja2

ENVIRONMENT = jinja2.Environment(
    extensions=["jinja2.ext.do", "jinja2.ext.loopcontrols"]
)

JINJA_METHODS = {
    "env_var": lambda key, default=None: os.getenv(key, default),
    "today": lambda: datetime.now().strftime("%Y-%m-%d"),
    "yesterday": lambda: (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d"),
}
