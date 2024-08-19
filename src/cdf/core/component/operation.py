from .base import Entrypoint

OperationProto = int


class Operation(Entrypoint[OperationProto], frozen=True):
    """A generic callable that returns an exit code."""
