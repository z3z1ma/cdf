import sys
import typing as t
from contextlib import contextmanager

A = t.TypeVar("A")
B = t.TypeVar("B")


@contextmanager
def _augmented_path(*path: str):
    """Temporarily append a path to sys.path.

    Args:
        *path: The path to append.

    Returns:
        A context manager that appends the path to sys.path and then restores the
        original path.
    """
    orig_path = sys.path[:]
    sys.path.extend(path)
    yield
    sys.path = orig_path


def do(fn: t.Callable[[A], B], it: t.Iterable[A]) -> t.List[B]:
    """Apply a function to an iterable.

    Unlike map, this function will force evaluation of the iterable.

    Args:
        fn: The function to apply.
        it: The iterable to apply the function to.

    Returns:
        A list of the results of applying the function to the iterable.
    """
    return list(map(fn, it))


__all__ = ["_augmented_path", "do"]
