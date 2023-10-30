"""The loader is responsible for importing cdf sources.

CDF sources export a constant named __CDF_SOURCE__ which captures a dict
of functions or closures that return a ContinuousDataFlowSource.
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
from cdf.core.exception import SourceDirectoryEmpty, SourceDirectoryNotFoundError
from cdf.core.source import ContinuousDataFlowSource
from cdf.core.utils import _augmented_path


def get_directory_modules(base_directory: Path | str) -> t.Iterable[ct.Loadable]:
    """Load all modules in the sources directory.

    Args:
        base_directory (Path | str): The base directory to load modules from.

    Raises:
        SourceDirectoryNotFoundError: If the base directory does not exist.
        SourceDirectoryNotFoundError: If the base directory is not a directory.

    Returns:
        t.Iterable[ct.Loadable]: An iterable of modules.
    """
    if isinstance(base_directory, str):
        base_directory = Path(base_directory)
    if not base_directory.exists():
        raise SourceDirectoryNotFoundError(f"{base_directory} does not exist.")
    if not base_directory.is_dir():
        raise SourceDirectoryNotFoundError(f"{base_directory} is not a directory.")
    paths = [p for p in base_directory.glob("*.py") if p.stem != "__init__"]
    if not paths:
        raise SourceDirectoryEmpty(f"{base_directory} contains no sources.")
    with _augmented_path(str(base_directory)):
        for path in paths:
            yield path


def _load_module_from_path(path: Path) -> ModuleType:
    """Load a module from a path.

    Args:
        path (Path): The path to the module.

    Returns:
        ModuleType: The loaded module.
    """
    spec = importlib.util.spec_from_file_location(path.stem, path)
    assert spec and spec.loader, f"Failed to create spec for {path}"
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _load_module_from_name(name: str) -> ModuleType:
    """Load a module from a name.

    Args:
        name (str): The name of the module as an import path.

    Returns:
        ModuleType: The loaded module.
    """
    return importlib.import_module(name)


def load_module(mod: ct.Loadable) -> ModuleType:
    """Load a module.

    Args:
        mod (ct.Loadable): The module to load.

    Raises:
        TypeError: If the module is not a valid type.

    Returns:
        ModuleType: The loaded module.
    """
    if isinstance(mod, ModuleType):
        result = ct.Result(mod)
    elif isinstance(mod, str):
        result = ct.Result.apply(_load_module_from_name, mod)
    elif isinstance(mod, Path):
        result = ct.Result.apply(_load_module_from_path, mod)
    else:
        raise TypeError(f"Invalid module type {type(mod)}")
    processed_mod, e = result
    if isinstance(e, Exception):
        raise e
    assert isinstance(processed_mod, ModuleType), f"Failed to load module {mod}"
    return processed_mod


def load_sources(
    get_modules_fn: t.Callable[[], t.Iterable[ct.Loadable]] = partial(
        get_directory_modules, Path("./sources")
    ),
    load_module_fn: t.Callable[[ct.Loadable], ModuleType] = load_module,
    cache: ct.SourceSpec | None = None,
    lazy_sources: bool = True,
    clear_linecache: bool = True,
) -> None:
    """Load all sources from the sources directory.

    Args:
        get_modules_fn (t.Callable[[], t.Iterable[ct.Loadable]], optional): A function
            that returns an iterable of modules. Defaults to partial(get_directory_modules, Path("./sources")).
        load_module_fn (t.Callable[[ct.Loadable], ModuleType], optional): A function that
            loads a module. Defaults to load_module.
        cache (ct.SourceSpec | None, optional): A dict to cache sources in. Defaults to None. This is
            mutable, so it is modified in-place.
        lazy_sources (bool, optional): Whether to lazy load sources. Defaults to True.
        clear_linecache (bool, optional): Whether to clear the linecache. Defaults to True.

    Raises:
        SourceDirectoryNotFoundError: If the base directory does not exist.
        SourceDirectoryNotFoundError: If the base directory is not a directory.
        SourceDirectoryEmpty: If the base directory contains no sources.

    Returns:
        None: Nothing.
    """
    cache = cache if cache is not None else {}
    if clear_linecache:
        linecache.clearcache()
    for module in get_modules_fn():
        mod = load_module_fn(module)
        source_fns: ct.SourceSpec = getattr(mod, c.CDF_SOURCE)
        cache.update(source_fns)
    if not lazy_sources:
        for source in cache.values():
            if not isinstance(source, ContinuousDataFlowSource):
                source()


__all__ = ["load_sources", "load_module", "get_directory_modules"]
