"""General utilities for the CDF package."""

import importlib
import os
import sys
import typing as t
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

__all__ = ["inject_sys_path", "change_dir", "resolve_entry_point"]


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


@contextmanager
def change_dir(target_dir: Path | str) -> Iterator[None]:
    """Temporarily change the current working directory. (not thread-safe)

    Args:
        target_dir (Path | str): The target directory to change to.

    Yields:
        None
    """
    original_dir = os.getcwd()
    try:
        os.chdir(target_dir)
        yield
    finally:
        os.chdir(original_dir)


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
