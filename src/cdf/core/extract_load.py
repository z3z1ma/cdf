# pyright: reportImportCycles=false, reportUnknownMemberType=false
"""Extract-load adapters for different ETL frameworks."""

from __future__ import annotations

import inspect
import logging
import subprocess  # nosec
import sys
import typing as t
from abc import ABC, abstractmethod
from contextlib import contextmanager
from fnmatch import fnmatch
from pathlib import Path
from types import ModuleType, TracebackType

from cdf.core.configuration import ConfigBox
from cdf.utils.file import load_module_from_path

__all__ = ["DltAdapter", "SlingAdapter", "SingerAdapter"]

T = t.TypeVar("T")

logger = logging.getLogger(__name__)


class ExtractLoadAdapterBase(ABC):
    """Abstract base class for all extract-load adapters."""

    def __init__(self, package_path: Path, config: t.Any) -> None:
        self.package_path: Path = package_path
        self.config: ConfigBox = config
        self._pipelines: dict[str, t.Callable[..., t.Any]] = {}

    @abstractmethod
    def discover_pipelines(self) -> dict[str, t.Callable[..., t.Any]]:
        """Discover available pipelines."""
        pass

    @abstractmethod
    def __call__(self, pipeline_name: str, **kwargs: t.Any) -> None:
        """Run a specific pipeline."""
        pass

    def __enter__(self) -> None:
        pass

    def __exit__(
        self, exc_type: type[BaseException], exc_val: BaseException, exc_tb: TracebackType
    ) -> None:
        pass

    def __getitem__(self, name: str) -> t.Callable[..., t.Any]:
        return (self._pipelines or self.discover_pipelines())[name]

    def __getattr__(self, name: str) -> t.Callable[..., t.Any]:
        try:
            return (self._pipelines or self.discover_pipelines())[name]
        except KeyError as e:
            raise AttributeError from e


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
    _pipelines: dict[str, DltPipelineProtocol]

    def discover_pipelines(self) -> dict[str, DltPipelineProtocol]:
        """Discover all extract-load pipelines in main.py."""
        if self._pipelines:
            return self._pipelines
        pipelines: dict[str, DltPipelineProtocol] = {}
        main_script = self.package_path / "main.py"
        if main_script.exists():
            with self._inject_provider():
                pipelines.update(self._load_functions_from_module(main_script, "pipeline_*"))
        else:
            logger.warning("No main.py found in package %s", self.package_path.stem)
        if not pipelines:
            logger.warning("No extract-load pipelines found in package %s", self.package_path.stem)
        self._pipelines = pipelines
        return pipelines

    def __call__(self, pipeline_name: str, **kwargs: t.Any) -> None:
        """Run a specific pipeline."""
        pipelines = self.discover_pipelines()
        if pipeline_name not in pipelines:
            raise ValueError(
                f"Pipeline {pipeline_name} not found in package {self.package_path.stem}, ensure it exists."
            )
        with self._inject_provider():
            logger.info("Running pipeline %s", pipeline_name)
            pipelines[pipeline_name](**kwargs)

    @contextmanager
    def _inject_provider(self):
        """Inject the CDF context into the DLT context for centralized configuration."""
        from dlt.common.configuration.container import Container
        from dlt.common.configuration.providers import CustomLoaderDocProvider
        from dlt.common.configuration.specs import PluggableRunContext
        from dlt.common.runtime.run_context import RunContext

        with Container().injectable_context(
            PluggableRunContext(RunContext(run_dir=str(self.package_path)))
        ) as dlt_context:
            provider_name = f"cdf.{self.package_path.name}.configuration"
            if provider_name not in dlt_context.providers:
                logger.debug("Injecting CDF configuration provider: %s", provider_name)
                dlt_context.providers.add_provider(
                    CustomLoaderDocProvider(provider_name, lambda: self.config)
                )
            yield
            logger.debug("Restoring DLT context")


class SlingPipelineProtocol(t.Protocol):
    def __call__(self, *args: t.Any, **kwds: t.Any) -> t.Any: ...


class SlingAdapter(ExtractLoadAdapterBase, ScriptLoaderMixin[SlingPipelineProtocol]):
    def discover_pipelines(self) -> dict[str, SlingPipelineProtocol]:
        """Discover all pipelines for sling."""
        pipelines: dict[str, SlingPipelineProtocol] = {}
        main_script = self.package_path / "main.py"
        if main_script.exists():
            pipelines.update(self._load_functions_from_module(main_script, "pipeline_*"))
        else:
            logger.warning("No main.py found in package %s", self.package_path.stem)
        if not pipelines:
            logger.warning("No extract-load pipelines found in package %s", self.package_path.stem)
        return pipelines

    def __call__(self, pipeline_name: str, **kwargs: t.Any) -> None:
        """Run a specific pipeline."""
        pipelines = self.discover_pipelines()
        if pipeline_name not in pipelines:
            raise ValueError(
                f"Pipeline {pipeline_name} not found in package {self.package_path.stem}, ensure it exists",
            )
        logger.info("Running pipeline %s", pipeline_name)
        pipelines[pipeline_name](**kwargs)


class SingerAdapter(ExtractLoadAdapterBase):
    def discover_pipelines(self) -> dict[str, t.Callable[..., t.Any]]:
        """Singer doesn't have callable pipelines; define commands."""
        return {"default_pipeline": self}

    def __call__(self, pipeline_name: str = "default_pipeline", **kwargs: t.Any) -> None:
        """Run a singer pipeline using subprocess."""
        tap = t.cast(str, self.config.get("singer_tap"))
        target = t.cast(str, self.config.get("singer_target"))
        if not tap or not target:
            raise ValueError("Singer adapter requires 'singer_tap' and 'singer_target' in config.")
        logger.info("Running Singer pipeline from %s to %s", tap, target)
        _ = subprocess.run(["echo", "1"], check=True)  # nosec
