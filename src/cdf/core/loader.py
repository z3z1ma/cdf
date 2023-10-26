"""The loader is responsible for importing cdf sources which triggers registration."""
import importlib
import importlib.util
import linecache
import os
import sys
import typing as t
from contextlib import contextmanager
from pathlib import Path
from types import ModuleType

from cdf.core.exception import SourceDirectoryEmpty, SourceDirectoryNotFoundError


@contextmanager
def _augmented_path(path: str):
    """Temporarily append a path to sys.path."""
    sys.path.append(path)
    yield
    sys.path.pop()


class SourceProto(t.Protocol):
    def setup() -> None:
        """Perform any setup required to register the source."""


class SourceLoader:
    """The loader is responsible for importing all modules in the sources directory."""

    def __init__(self, base_directory: str = "./sources", load: bool = True) -> None:
        self._modules: t.Dict[str, ModuleType] = {}
        self._base_directory = Path(base_directory).resolve()
        self._executions = 0
        if load:
            self.load()

    def load(self) -> None:
        """Load all modules in the sources directory."""
        self._modules = {}
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
        if self._executions > 0:
            linecache.clearcache()
        with _augmented_path(str(self._base_directory)):
            for path in paths:
                spec = importlib.util.spec_from_file_location(path.stem, path)
                assert spec and spec.loader, f"Failed to create spec for {path}"
                src = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(src)
                t.cast(SourceProto, src).setup()  # Side-effect: registers source
                self._modules[src.__name__] = src
        self._executions += 1

    def get_module(self, name: str):
        """Get a module from the sources directory."""
        return self._modules[name]
