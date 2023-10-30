import sys
from contextlib import contextmanager


@contextmanager
def _augmented_path(*path: str):
    """Temporarily append a path to sys.path."""
    orig_path = sys.path[:]
    sys.path.extend(path)
    yield
    sys.path = orig_path


__all__ = ["_augmented_path"]
