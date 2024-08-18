from .base import Entrypoint


class Operation(Entrypoint[int], frozen=True):
    """A generic callable that returns an exit code."""
