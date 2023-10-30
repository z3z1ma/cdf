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
    """Load all modules in the sources directory."""
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


def load_module(mod: ct.Loadable) -> ModuleType:
    if isinstance(mod, str):
        mod_ns = importlib.import_module(mod)
    elif isinstance(mod, Path):
        spec = importlib.util.spec_from_file_location(mod.stem, mod)
        assert spec and spec.loader, f"Failed to create spec for {mod}"
        mod_ns = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod_ns)
    elif isinstance(mod, ModuleType):
        mod_ns = mod
    else:
        raise TypeError(f"Invalid module type {type(mod)}")
    return mod_ns


def load_sources(
    get_modules_fn: t.Callable[[], t.Iterable[ct.Loadable]] = partial(
        get_directory_modules, Path("./sources")
    ),
    load_module_fn: t.Callable[[ct.Loadable], ModuleType] = load_module,
    cache: ct.SourceSpec | None = None,
    lazy_sources: bool = True,
    clear_linecache: bool = True,
) -> None:
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
