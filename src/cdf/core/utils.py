"""Utility functions for the cdf package."""
import functools
import json
import sys
import types
import typing as t
from contextlib import contextmanager, suppress
from importlib.machinery import ModuleSpec
from importlib.util import module_from_spec, spec_from_file_location
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


def deep_merge(lhs: dict, rhs: dict) -> dict:
    """
    Deep merges two dictionaries. If a key is present in both dictionaries:
      - If the values are both dicts, they are merged recursively.
      - If both are lists, they are concatenated.
      - Otherwise, the value from dict2 will overwrite the one in dict1.

    Args:
        lhs: The first dictionary.
        rhs: The second dictionary.

    Returns:
        The merged dictionary.
    """
    merged = dict(lhs)

    for key, value in rhs.items():
        if key in merged:
            if isinstance(merged[key], dict) and isinstance(value, dict):
                merged[key] = deep_merge(merged[key], value)
            elif isinstance(merged[key], list) and isinstance(value, list):
                merged[key] = merged[key] + value
            else:
                merged[key] = value
        else:
            merged[key] = value

    return merged


def load_module_from_path(
    path: Path, execute: bool = True, /, package: str | None = None
) -> t.Tuple[types.ModuleType, ModuleSpec]:
    """Load a module from a path.

    Args:
        path: The path to the module.
        execute: Whether to execute the module.

    Returns:
        A tuple of the module and the module spec.
    """
    spec = spec_from_file_location(path.stem, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Could not load source {path}")
    module = module_from_spec(spec)
    module.__file__ = str(path)
    if package:
        module.__package__ = package
    sys.modules[spec.name] = module
    if execute:
        spec.loader.exec_module(module)
    return module, spec


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


def fn_to_str(fn: t.Callable | functools.partial | DltSource) -> str:
    """Convert a function to a string representation."""
    if isinstance(fn, DltSource):
        return f"object: DltSource({fn.name}), id: {id(fn)}"
    if isinstance(fn, functools.partial):
        fn = fn.func
    if hasattr(fn, "__wrapped__"):
        fn = t.cast(t.Callable, fn.__wrapped__)
    parts = [
        f"mod: {fn.__module__}",
        f"fn: {fn.__name__}",
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
