"""Utility functions for the cdf package."""
import functools
import json
import os
import sys
import typing as t
from contextlib import contextmanager, suppress
from pathlib import Path

from cdf.core import constants as c
from cdf.core import types as ct

A = t.TypeVar("A")
B = t.TypeVar("B")


@contextmanager
def augmented_path(*path: str):
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


def index_destinations(
    environment: t.Dict[str, str] | None = None
) -> ct.DestinationSpec:
    """Index destinations from the environment based on a standard convention.

    Notes:
        Convention is as follows:

        CDF_<DESTINATION_NAME>__<ENGINE_NAME>=<NATIVE VALUE>
        CDF_<DESTINATION_NAME>__<ENGINE_NAME>__<KEY>=<VALUE>

    Args:
        environment: The environment to index. Defaults to os.environ.copy().

    Returns:
        A dict of destination names to tuples of engine names and credentials.
    """
    environment = environment or os.environ.copy()
    destinations: ct.DestinationSpec = {
        "default": ct.EngineCredentials("duckdb", "duckdb:///cdf.db"),
    }
    for k, v in environment.items():
        dest_key = c.DEST_CRED_PAT.match(k)
        native_dest = c.NATIVE_DEST_CRED_PAT.match(k)
        if not (dest_key or native_dest):
            continue
        parts = k[4:].split("__")
        if len(parts) == 2:
            dest, engine = parts
            destinations[dest.lower()] = ct.EngineCredentials(engine.lower(), v)
        elif len(parts) == 3:
            dest, engine, key = parts
            if dest.lower() not in destinations:
                destinations[dest.lower()] = ct.EngineCredentials(engine.lower(), {})
            t.cast(dict, destinations[dest.lower()].credentials)[key.lower()] = v
    return destinations


def fn_to_str(fn: t.Callable | functools.partial) -> str:
    """Convert a function to a string representation."""
    if isinstance(fn, functools.partial):
        fn = fn.func
    parts = [
        f"mod: [cyan]{fn.__module__}[/cyan]",
        f"fn: [yellow]{fn.__name__}[/yellow]",
        f"ln: {fn.__code__.co_firstlineno}",
    ]
    return ", ".join(parts)


def flatten_stream(it: t.Iterable[A | t.List[A] | t.Tuple[A]]) -> t.Iterator[A]:
    """Flatten a stream of iterables."""
    for i in it:
        if isinstance(i, (list, tuple)):
            yield from flatten_stream(i)
        else:
            yield i


def search_merge_json(path: Path, fname: str, max_depth: int = 3) -> t.Dict[str, t.Any]:
    """Search for and merge json files.

    Args:
        path: Path to start searching from.
        fname: Name of the json file to search for.
        max_depth: Maximum depth to search.

    Returns:
        dict: A dict of the merged json files.
    """
    obj = {}
    if not path.exists():
        return obj
    traverse_path = path
    local_depth = 0
    while traverse_path != Path("/"):
        f = traverse_path / fname
        traverse_path = traverse_path.parent
        if not f.exists():
            continue
        with suppress(json.JSONDecodeError):
            obj.update(json.loads(f.read_text()))
        local_depth += 1
        if local_depth >= max_depth:
            break
    return obj


__all__ = [
    "augmented_path",
    "do",
    "index_destinations",
    "fn_to_str",
    "flatten_stream",
    "search_merge_json",
]
