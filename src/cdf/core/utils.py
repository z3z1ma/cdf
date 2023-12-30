"""Utility functions for the cdf package."""
import functools
import inspect
import json
import sys
import types
import typing as t
from contextlib import contextmanager, suppress
from importlib.machinery import ModuleSpec
from importlib.util import module_from_spec, spec_from_file_location, spec_from_loader
from pathlib import Path

import dlt
from dlt.sources import DltResource, DltSource

if t.TYPE_CHECKING:
    from cdf.core.spec import PipelineInterface

A = t.TypeVar("A")
B = t.TypeVar("B")


def populate_fn_kwargs_from_config(
    fn: t.Callable[..., t.Any],
    kwargs: t.Dict[str, t.Any],
    sig_excludes: t.Set[str] | None = None,
    config_path: t.Tuple[str, ...] = (),
) -> t.Dict[str, t.Any]:
    """
    Given a function `fn` and a dict of kwargs `kwargs`, populate the kwargs with values
    from the config based on the function signature. For each kwarg in the function signature
    that is not present in the kwargs, the config will be searched for a value at the path prefixed
    by the config path. For example, if the config path is ["ff", "harness"] and the kwarg is "foo",
    the config will be searched for a value at ["ff", "harness", "foo"]. If the value is found, it
    will be added to the kwargs.

    Args:
        fn: The function.
        kwargs: The kwargs to populate. Mutated in place.
        private_attrs: A set of private attributes to exclude.
        config_path: The path to the config. IE ["ff", "harness"]

    Returns:
        The kwargs supplemented by the config providers.
    """
    sig_excludes = sig_excludes or set()
    fn_kwargs = inspect.signature(fn).parameters.keys() - sig_excludes
    for k in fn_kwargs:
        if k not in kwargs:
            with suppress(KeyError):
                kwargs[k] = dlt.config[".".join([*config_path, k])]
            with suppress(KeyError):
                kwargs[k] = dlt.secrets[".".join([*config_path, k])]
    return kwargs


@contextmanager
def augmented_path(*path: str):
    """
    Temporarily append a path to sys.path.

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


def load_module_from_string(name: str, source: str, /, package: str | None = None):
    spec = spec_from_loader(name, loader=None)
    if spec is None:
        raise RuntimeError(f"Could not load source {name}")
    module = module_from_spec(spec)
    if package:
        module.__package__ = package
    sys.modules[spec.name] = module
    exec(source, module.__dict__)
    return module, spec


def load_module_from_path(
    path: str | Path, execute: bool = True, /, package: str | None = None
) -> t.Tuple[types.ModuleType, ModuleSpec]:
    """Load a module from a path.

    Args:
        path: The path to the module.
        execute: Whether to execute the module.

    Returns:
        A tuple of the module and the module spec.
    """
    if isinstance(path, str):
        path = Path(path)
    spec = spec_from_file_location(path.stem, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Could not load source {path}")
    module = module_from_spec(spec)
    module.__file__ = str(path)
    if package:
        module.__package__ = package
    sys.modules[spec.name] = module
    if execute:
        # exec(compile(open(path).read(), str(path), "exec"), module.__dict__)
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
    "flatten_stream",
    "search_merge_json",
    "get_source_component_id",
    "qualify_source_component_id",
]
