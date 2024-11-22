"""General utilities for the CDF package."""

import os
import sys
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

__all__ = ["inject_sys_path", "change_dir"]


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
