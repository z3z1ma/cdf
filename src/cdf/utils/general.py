import sys
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

__all__ = ["inject_sys_path"]


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
