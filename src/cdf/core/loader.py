"""The loader is responsible for importing cdf sources.

CDF sources export a constant named __CDF_SOURCE__ which captures a dict
of functions or closures that return a CDFSource.

The main entrypoint is `populate_source_cache` which is composable and can be supplied
with a custom `get_modules_fn` and `load_module_fn`. The default implementation of these
functions is `get_directory_modules` and `load_module` respectively. These will load all
modules in the `./sources` subdirectory relative to the current working directory.
"""
import importlib
import importlib.util
import linecache
import typing as t
from functools import partial
from pathlib import Path
from types import ModuleType

import cdf.core.constants as c
import cdf.core.types as ct
from cdf.core.utils import augmented_path, do


def get_directory_modules(base_directory: Path | str) -> t.Iterable[ct.Loadable]:
    """Load all modules in the sources directory.

    Args:
        base_directory: The base directory to load modules from.

    Raises:
        SourceDirectoryNotFoundError: If the base directory does not exist.
        SourceDirectoryNotFoundError: If the base directory is not a directory.

    Returns:
        An iterable of modules.
    """
    if isinstance(base_directory, str):
        base_directory = Path(base_directory)
    if base_directory.exists() and base_directory.is_file():
        base_directory = base_directory.parent
    paths = [p for p in base_directory.glob("*.py") if p.stem != "__init__"]
    if not paths:
        return None
    with augmented_path(str(base_directory)):
        for path in paths:
            yield path


def _load_module_from_path(path: Path) -> ModuleType:
    """Load a module from a path.

    Args:
        path: The path to the module.

    Returns:
        The loaded module.
    """
    spec = importlib.util.spec_from_file_location(path.stem, path)
    assert spec and spec.loader, f"Failed to create spec for {path}"
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _load_module_from_name(name: str) -> ModuleType:
    """Load a module from a name.

    Args:
        name: The name of the module as an import path.

    Returns:
        The loaded module.
    """
    return importlib.import_module(name)


def load_module(mod: ct.Loadable) -> ModuleType:
    """Load a module.

    Args:
        mod: The module to load.

    Raises:
        TypeError: If the module is not a valid type.

    Returns:
        The loaded module.
    """
    if isinstance(mod, ModuleType):
        result = ct.Result(mod)
    elif isinstance(mod, str):
        result = ct.Result.apply(_load_module_from_name, mod)
    elif isinstance(mod, Path):
        result = ct.Result.apply(_load_module_from_path, mod)
    else:
        raise TypeError(f"Invalid module type {type(mod)}")
    return result.expect()


def populate_source_cache(
    cache: ct.SourceSpec | None = None,
    /,
    get_modules_fn: t.Callable[[], t.Iterable[ct.Loadable]] = partial(
        get_directory_modules, Path("./sources")
    ),
    load_module_fn: t.Callable[[ct.Loadable], ModuleType] = load_module,
    clear_linecache: bool = True,
) -> ct.SourceSpec:
    """Load all sources from the sources directory.

    Args:
        cache: A dict to cache sources in. Defaults to None. This is modified in-place.
            If None, a new dict is created.
        get_modules_fn: A function that returns an iterable of modules.
            Defaults to partial(get_directory_modules, Path("./sources")).
        load_module_fn: A function that loads a module. Defaults to load_module.
        clear_linecache: Whether to clear the linecache. Defaults to True.

    Returns:
        The populated cache.
    """
    cache = cache if cache is not None else {}
    if clear_linecache:
        linecache.clearcache()
    do(
        cache.update,
        map(
            lambda mod: getattr(load_module_fn(mod), c.CDF_SOURCE, {}), get_modules_fn()
        ),
    )
    return cache


__all__ = ["populate_source_cache", "load_module", "get_directory_modules"]
