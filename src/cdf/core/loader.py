"""The loader is responsible for importing cdf sources & running setup.

This process triggers ambient component registration.
"""
import abc
import importlib
import importlib.util
import linecache
import sys
import typing as t
from contextlib import contextmanager
from pathlib import Path
from types import ModuleType

import cdf.core.constants as c
from cdf.core.exception import SourceDirectoryEmpty, SourceDirectoryNotFoundError
from cdf.core.source import ContinuousDataFlowSource

Loadable = t.Union[str, Path, ModuleType]
LazySource = t.Callable[[], ContinuousDataFlowSource]
SourceSpec = t.Dict[str, LazySource]


@contextmanager
def _augmented_path(path: str):
    """Temporarily append a path to sys.path."""
    sys.path.append(path)
    yield
    sys.path.pop()


class SourceLoader(abc.ABC):
    """An abstract base class for source loaders."""

    def __init__(self, cache: SourceSpec | None = None, load: bool = True) -> None:
        self.cache = cache or {}
        self.executions = 0
        if load:
            self.load()

    def load(self) -> None:
        """Load all source modules populating the cache."""
        self.cache = {}
        if self.executions > 0:
            linecache.clearcache()
        for module in self.get_modules():
            self._load_module(module)

    def _load_module(self, module: Loadable) -> None:
        """Load a Loadable object."""
        if isinstance(module, str):
            module_ns = importlib.import_module(module)
        elif isinstance(module, Path):
            spec = importlib.util.spec_from_file_location(module.stem, module)
            assert spec and spec.loader, f"Failed to create spec for {module}"
            module_ns = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module_ns)
        elif isinstance(module, ModuleType):
            module_ns = module
        source_fns: SourceSpec = getattr(module_ns, c.CDF_SOURCE)
        self.cache.update(source_fns)

    @abc.abstractmethod
    def get_modules(self) -> t.Iterable[Loadable]:
        """Get all modules.

        This method should return an iterable of Loadable objects. Given a str, we will assume it
        is an abs import such as my.module.source, if it is a Path, we will assume it is either
        a path to a python file or directory of files, and if it is a ModuleType, we will assume
        it is a module object. All imports must contain __CDF_SOURCE__ = fn()|[fn()] to be loaded.

        Returns:
            An iterable of Loadable objects.
        """
        raise NotImplementedError


class DirectoryLoader(SourceLoader):
    """The loader is responsible for importing all modules in the sources directory."""

    def __init__(
        self,
        base_directory: str = "./sources",
        /,
        cache: SourceSpec | None = None,
        load: bool = True,
    ) -> None:
        super().__init__(cache=cache, load=False)
        self._base_directory = Path(base_directory).resolve()
        if load:
            self.load()

    def get_modules(self) -> t.Iterable[Loadable]:
        """Load all modules in the sources directory."""
        if not self._base_directory.exists():
            raise SourceDirectoryNotFoundError(
                f"{self._base_directory} does not exist."
            )
        if not self._base_directory.is_dir():
            raise SourceDirectoryNotFoundError(
                f"{self._base_directory} is not a directory."
            )
        paths = [p for p in self._base_directory.glob("*.py") if p.stem != "__init__"]
        if not paths:
            raise SourceDirectoryEmpty(f"{self._base_directory} contains no sources.")
        with _augmented_path(str(self._base_directory)):
            for path in paths:
                yield path


__all__ = ["LazySource", "Loadable", "SourceSpec", "SourceLoader", "DirectoryLoader"]
