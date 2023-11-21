import os

import jinja2

ENVIRONMENT = jinja2.Environment(
    extensions=["jinja2.ext.do", "jinja2.ext.loopcontrols"]
)

JINJA_METHODS = {"env_var": lambda key, default=None: os.getenv(key, default)}
