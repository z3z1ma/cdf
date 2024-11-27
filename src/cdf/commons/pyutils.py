"""Python-specific utilities for the CDF package."""

import importlib
import sys
import typing as t
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

__all__ = ["inject_sys_path", "resolve_entry_point", "unique_dict"]

T = t.TypeVar("T")
K = t.TypeVar("K", bound=t.Hashable)


@contextmanager
def inject_sys_path(*paths: Path | str, prepend: bool = True) -> Iterator[None]:
    """Temporarily add paths to sys.path.

    Args:
        paths (List[str]): List of paths to temporarily add to sys.path.

    Yields:
        None
    """
    original_sys_path = sys.path[:]
    strpaths = list(map(lambda p: str(Path(p).resolve()), paths))
    try:
        if prepend:
            sys.path[:0] = strpaths
        else:
            sys.path.extend(strpaths)
        yield
    finally:
        sys.path = original_sys_path


def resolve_entry_point(value: str) -> t.Callable[[str], t.Any]:
    """Resolves a string in 'module:function' format to a callable."""
    module_path, _, function_name = value.partition(":")
    if not module_path or not function_name:
        raise ValueError(
            f"Invalid entry point '{value}'. It must be in 'module.path:function_name' format."
        )
    try:
        module = importlib.import_module(module_path)
        func = getattr(module, function_name)
        if not callable(func):
            raise TypeError(f"'{function_name}' is not callable in module '{module_path}'.")
        return func
    except (ImportError, AttributeError, TypeError) as e:
        raise ValueError(f"Error resolving entry point '{value}': {e}") from e


@t.final
class unique_dict(dict[K, T]):
    """Dict that raises when a duplicate key is set."""

    def __init__(self, name: str, *args: dict[K, T], **kwargs: T) -> None:
        self.name = name
        super().__init__(*args, **kwargs)

    def __setitem__(self, k: K, v: T) -> None:
        if k in self:
            raise ValueError(
                f"Duplicate key '{k}' found in unique_dict<{self.name}>. Call dict.update(...) if this is intentional."
            )
        super().__setitem__(k, v)
