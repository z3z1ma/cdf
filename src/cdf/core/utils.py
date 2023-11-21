"""Utility functions for the cdf package."""
import functools
import json
import sys
import typing as t
from contextlib import contextmanager, suppress
from pathlib import Path

from dlt.sources import DltResource, DltSource

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
    depth = 0
    while path.parents and depth <= max_depth:
        f = path / fname
        path = path.parent
        depth += 1
        if not f.exists():
            continue
        with suppress(json.JSONDecodeError):
            obj.update(json.loads(f.read_text()))
    return obj


def get_source_component_id(
    source: DltSource,
    resource: str | DltResource | None = None,
    workspace: str | None = None,
) -> str:
    """Convert a source object and resource object or str into a canonicalized representation"""
    src_str = source.name if not workspace else f"{workspace}.{source.name}"
    parts = ["source", src_str]
    if resource:
        _resource = (
            source.resources[resource] if isinstance(resource, str) else resource
        )
        parts.append(_resource.name)
    return ":".join(parts)


def qualify_source_component_id(
    component_id: str,
    workspace: str | None = None,
) -> str:
    """Ensure a component id is qualified with a workspace and starts with source:"""
    src = component_id
    if not component_id.startswith("source:"):
        src = f"source:{component_id}"
    try:
        typ, src, res = component_id.split(":", 2)
        if "." not in src and workspace:
            src = f"{workspace}.{src}"
    except ValueError:
        return src
    return f"{typ}:{src}:{res}"


__all__ = [
    "augmented_path",
    "do",
    "fn_to_str",
    "flatten_stream",
    "search_merge_json",
    "get_source_component_id",
    "qualify_source_component_id",
]
