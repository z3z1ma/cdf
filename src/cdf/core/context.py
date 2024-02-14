"""Context module."""
from contextvars import ContextVar

logger = ContextVar("logger", default=None)

replace = ContextVar("replace", default=False)
