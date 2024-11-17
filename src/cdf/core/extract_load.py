# pyright: reportImportCycles=false, reportUnknownMemberType=false
"""Extract-load adapters for different ETL frameworks."""

from __future__ import annotations

import inspect
import subprocess  # nosec
import sys
import typing as t
from abc import ABC, abstractmethod
from fnmatch import fnmatch
from pathlib import Path
from types import ModuleType

from cdf.core.configuration import ConfigBox
from cdf.utils.file import load_module_from_path


class ExtractLoadAdapterBase(ABC):
    """Abstract base class for all extract-load adapters."""

    def __init__(self, package_path: Path, config: ConfigBox) -> None:
        self.package_path: Path = package_path
        self.config: ConfigBox = config

    @abstractmethod
    def discover_pipelines(self) -> dict[str, t.Callable[..., t.Any]]:
        """Discover available pipelines."""
        pass

    @abstractmethod
    def run_pipeline(self, pipeline_name: str, **kwargs: t.Any) -> None:
        """Run a specific pipeline."""
        pass


T = t.TypeVar("T")


class ScriptLoaderMixin(t.Generic[T]):
    def _load_module(self, module_path: Path | str) -> ModuleType:
        """Load a module from the package directory."""
        path = Path(module_path)
        sys.path.insert(0, str(path.parent))
        try:
            return load_module_from_path(path)
        finally:
            _ = sys.path.pop(0)

    def _load_functions_from_module(
        self,
        script_path: Path,
        func_glob: str = "*",
    ) -> dict[str, t.Callable[..., T]]:
        """Load all callable functions from a module."""
        module = self._load_module(script_path)
        functions = {
            name: obj
            for name, obj in inspect.getmembers(module, inspect.isfunction)
            if not inspect.isbuiltin(obj)
            and inspect.getmodule(inspect.unwrap((obj))) in (module, None)
            and fnmatch(name, func_glob)
        }
        return functions


class DltPipelineProtocol(t.Protocol):
    def __call__(self, *args: t.Any, **kwds: t.Any) -> t.Any: ...


class DltAdapter(ExtractLoadAdapterBase, ScriptLoaderMixin[DltPipelineProtocol]):
    def discover_pipelines(self) -> dict[str, DltPipelineProtocol]:
        """Discover all extract-load pipelines in main.py."""
        pipelines: dict[str, DltPipelineProtocol] = {}
        main_script = self.package_path / "main.py"
        if main_script.exists():
            pipelines.update(self._load_functions_from_module(main_script, "pipeline_*"))

        return pipelines

    def run_pipeline(self, pipeline_name: str, **kwargs: t.Any) -> None:
        """Run a specific pipeline."""
        pipelines = self.discover_pipelines()
        if pipeline_name not in pipelines:
            raise ValueError(
                f"Pipeline {pipeline_name} not found in package {self.package_path.stem}, ensure it exists."
            )
        pipelines[pipeline_name](**kwargs)


class SlingPipelineProtocol(t.Protocol):
    def __call__(self, *args: t.Any, **kwds: t.Any) -> t.Any: ...


class SlingAdapter(ExtractLoadAdapterBase, ScriptLoaderMixin[SlingPipelineProtocol]):
    def discover_pipelines(self) -> dict[str, SlingPipelineProtocol]:
        """Discover all pipelines for sling."""
        pipelines: dict[str, SlingPipelineProtocol] = {}
        main_script = self.package_path / "main.py"
        if main_script.exists():
            pipelines.update(self._load_functions_from_module(main_script, "pipeline_*"))

        return pipelines

    def run_pipeline(self, pipeline_name: str, **kwargs: t.Any) -> None:
        """Run a specific pipeline."""
        pipelines = self.discover_pipelines()
        if pipeline_name not in pipelines:
            raise ValueError(
                f"Pipeline {pipeline_name} not found in package {self.package_path.stem}, ensure it exists",
            )
        pipelines[pipeline_name](**kwargs)


class SingerAdapter(ExtractLoadAdapterBase):
    def discover_pipelines(self) -> dict[str, t.Callable[..., t.Any]]:
        """Singer doesn't have callable pipelines; define commands."""
        return {"default_pipeline": self.run_pipeline}

    def run_pipeline(self, pipeline_name: str = "default_pipeline", **kwargs: t.Any) -> None:
        """Run a singer pipeline using subprocess."""
        tap = t.cast(str, self.config.get("singer_tap"))
        target = t.cast(str, self.config.get("singer_target"))
        if not tap or not target:
            raise ValueError("Singer adapter requires 'singer_tap' and 'singer_target' in config.")
        _ = subprocess.run(["echo", "1"], check=True)  # nosec
