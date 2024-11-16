# pyright: reportImportCycles=false, reportUnknownMemberType=false
"""Extract-load adapters for different ETL frameworks."""

from __future__ import annotations

import inspect
import subprocess  # nosec
import sys
import typing as t
from abc import ABC, abstractmethod
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

    def _load_module(self, module_path: Path | str) -> ModuleType:
        """Load a module from the package directory."""
        sys.path.insert(0, str(self.package_path))
        try:
            return load_module_from_path(module_path)
        finally:
            _ = sys.path.pop(0)

    def load_functions_from_module(self, script_path: Path) -> dict[str, t.Callable[..., t.Any]]:
        """Load all callable functions from a module."""
        module = self._load_module(script_path)
        functions = {
            name: obj
            for name, obj in inspect.getmembers(module, inspect.isfunction)
            if inspect.getmodule(obj) == module
        }
        return functions


class DltAdapter(ExtractLoadAdapterBase):
    def discover_pipelines(self) -> dict[str, t.Callable[..., t.Any]]:
        """Discover all extract-load pipelines in main.py."""
        pipelines: dict[str, t.Callable[..., t.Any]] = {}
        main_script = self.package_path / "main.py"
        if main_script.exists():
            pipelines.update(self.load_functions_from_module(main_script))

        return pipelines

    def run_pipeline(self, pipeline_name: str, **kwargs: t.Any) -> None:
        """Run a specific pipeline."""
        pipelines: dict[str, t.Callable[..., t.Any]] = self.discover_pipelines()
        if pipeline_name not in pipelines:
            raise ValueError(
                f"Pipeline {pipeline_name} not found in package {self.package_path.stem}"
            )
        pipelines[pipeline_name](**kwargs)


class SlingAdapter(ExtractLoadAdapterBase):
    def discover_pipelines(self) -> dict[str, t.Callable[..., t.Any]]:
        """Discover all pipelines for sling."""
        pipelines: dict[str, t.Callable[..., t.Any]] = {}
        main_script = self.package_path / "main.py"
        if main_script.exists():
            pipelines.update(self.load_functions_from_module(main_script))

        return pipelines

    def run_pipeline(self, pipeline_name: str, **kwargs: t.Any) -> None:
        """Run a specific pipeline."""
        pipelines: dict[str, t.Callable[..., t.Any]] = self.discover_pipelines()
        if pipeline_name not in pipelines:
            raise ValueError(
                f"Pipeline {pipeline_name} not found in package {self.package_path.stem}"
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
