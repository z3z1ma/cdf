# pyright: reportImportCycles=false, reportUnknownMemberType=false
"""Extract-load adapters for different ETL frameworks."""

from __future__ import annotations

import inspect
import logging
import subprocess  # nosec
import tempfile
import typing as t
from abc import ABC, abstractmethod
from collections.abc import Mapping
from contextlib import contextmanager
from fnmatch import fnmatch
from pathlib import Path
from types import ModuleType

from cdf.core.configuration import ConfigBox
from cdf.core.models import (
    DltAdapterConfig,
    ExtractLoadConfig,
    HamiltonAdapterConfig,
    SingerAdapterConfig,
    SlingAdapterConfig,
)
from cdf.utils.files import json, load_module_from_path, yaml
from cdf.utils.general import inject_sys_path

__all__ = ["DltAdapter", "SlingAdapter", "SingerAdapter", "HamiltonAdapter"]

T = t.TypeVar("T")
TConfig = t.TypeVar("TConfig", bound=ExtractLoadConfig)

logger = logging.getLogger(__name__)


@t.overload
def extract_load_adapter_factory(
    package_path: Path, adapter_conf: DltAdapterConfig, package_conf: ConfigBox
) -> DltAdapter: ...


@t.overload
def extract_load_adapter_factory(
    package_path: Path, adapter_conf: SingerAdapterConfig, package_conf: ConfigBox
) -> SingerAdapter: ...


@t.overload
def extract_load_adapter_factory(
    package_path: Path, adapter_conf: SlingAdapterConfig, package_conf: ConfigBox
) -> SlingAdapter: ...


@t.overload
def extract_load_adapter_factory(
    package_path: Path, adapter_conf: HamiltonAdapterConfig, package_conf: ConfigBox
) -> HamiltonAdapter: ...


def extract_load_adapter_factory(
    package_path: Path, adapter_conf: ExtractLoadConfig, package_conf: ConfigBox
) -> ExtractLoadAdapterBase[t.Any, t.Any]:
    match adapter_conf.adapter:
        case "dlt":
            return DltAdapter(package_path, adapter_conf, package_conf)
        case "singer":
            return SingerAdapter(package_path, adapter_conf, package_conf)
        case "sling":
            return SlingAdapter(package_path, adapter_conf, package_conf)
        case "hamilton":
            return HamiltonAdapter(package_path, adapter_conf, package_conf)


class ExtractLoadAdapterBase(ABC, t.Generic[T, TConfig]):
    """Abstract base class for all extract-load adapters."""

    def __init__(self, package_path: Path, adapter_conf: TConfig, package_conf: ConfigBox) -> None:
        self.package_path: Path = package_path
        self.adapter_conf: TConfig = adapter_conf
        self.package_conf: ConfigBox = package_conf
        self._pipelines: Mapping[str, T] = {}

    @abstractmethod
    def _discover_pipelines(self) -> Mapping[str, T]:
        """Discover available pipelines."""
        pass

    def discover_pipelines(self) -> Mapping[str, T]:
        """Discover available pipelines."""
        if not self._pipelines:
            self._pipelines = self._discover_pipelines()
        return self._pipelines

    @abstractmethod
    def __call__(self, pipeline_name: str, **kwargs: t.Any) -> None:
        """Run a specific pipeline."""
        pass

    def __getitem__(self, name: str) -> T:
        return (self._pipelines or self._discover_pipelines())[name]

    def __getattr__(self, name: str) -> T:
        try:
            return (self._pipelines or self._discover_pipelines())[name]
        except KeyError as e:
            raise AttributeError from e

    def _load_module(self, module_path: Path | str) -> ModuleType:
        """Load a module from the package directory."""
        path = Path(module_path)
        with inject_sys_path(path.parent):
            return load_module_from_path(path)

    def _load_functions_from_module(
        self,
        script_path: Path,
        func_glob: str = "*",
    ) -> Mapping[str, T]:
        """Load all callable functions from a module."""
        module = self._load_module(script_path)
        functions = {
            name: t.cast(T, obj)
            for name, obj in inspect.getmembers(module, inspect.isfunction)
            if not inspect.isbuiltin(obj)
            and inspect.getmodule(inspect.unwrap((obj))) in (module, None)
            and fnmatch(name, func_glob)
        }
        return functions


class DltPipelineProtocol(t.Protocol):
    def __call__(self, *args: t.Any, **kwds: t.Any) -> t.Any: ...


class DltAdapter(ExtractLoadAdapterBase[DltPipelineProtocol, DltAdapterConfig]):
    def _discover_pipelines(self) -> Mapping[str, DltPipelineProtocol]:
        """Discover all extract-load pipelines in main.py."""
        pipelines: dict[str, DltPipelineProtocol] = {}
        main_script = self.package_path / "main.py"
        if main_script.exists():
            with self._inject_provider():
                pipelines.update({
                    k[9:]: v
                    for k, v in self._load_functions_from_module(main_script, "pipeline_*").items()
                })
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
                f"Pipeline {pipeline_name} not found in package {self.package_path.stem}, available: {list(pipelines)}"
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
                    CustomLoaderDocProvider(provider_name, lambda: self.package_conf)
                )
            yield
            logger.debug("Restoring DLT context")


class SlingPipelineProtocol(t.Protocol):
    def __call__(self, *args: t.Any, **kwds: t.Any) -> t.Any: ...


class SlingAdapter(ExtractLoadAdapterBase[SlingPipelineProtocol, SlingAdapterConfig]):
    def _discover_pipelines(self) -> Mapping[str, SlingPipelineProtocol]:
        """Discover all pipelines for sling."""
        return {"main": self}

    def __call__(self, pipeline_name: str = "main", **kwargs: t.Any) -> None:
        """Run a specific pipeline."""
        logger.info(
            "Running Sling pipeline from %s to %s",
            self.adapter_conf.source,
            self.adapter_conf.target,
        )
        replication_conf = self.adapter_conf.model_dump(
            exclude={"adapter"}, exclude_none=True, by_alias=True
        )
        with tempfile.NamedTemporaryFile("w") as f:
            yaml.dump(replication_conf, f)
            f.flush()
            _ = subprocess.run(["sling", "run", "-r", f.name], check=True)


class SingerPipelineProtocol(t.Protocol):
    def __call__(self, *args: t.Any, **kwds: t.Any) -> t.Any: ...


class SingerAdapter(ExtractLoadAdapterBase[SingerPipelineProtocol, SingerAdapterConfig]):
    def _discover_pipelines(self) -> dict[str, t.Callable[..., t.Any]]:
        """Singer doesn't have callable pipelines; define commands."""
        return {"main": self}

    def __call__(self, pipeline_name: str = "main", **kwargs: t.Any) -> None:
        """Run a singer pipeline using subprocess."""
        logger.info(
            "Running Singer pipeline from %s to %s",
            self.adapter_conf.tap,
            self.adapter_conf.target,
        )
        with (
            tempfile.NamedTemporaryFile("w", suffix=".json") as tap_file,
            tempfile.NamedTemporaryFile("w", suffix=".json") as target_file,
        ):
            json.dump(self.adapter_conf.tap_config, tap_file)
            tap_file.flush()
            json.dump(self.adapter_conf.target_config, target_file)
            target_file.flush()

            tap_command = ["tap-" + self.adapter_conf.tap, "--config", tap_file.name]
            target_command = ["target-" + self.adapter_conf.target, "--config", target_file.name]

            try:
                tap_process = subprocess.Popen(tap_command, stdout=subprocess.PIPE)  # nosec
                _ = subprocess.run(target_command, stdin=tap_process.stdout, check=True)  # nosec
                _ = tap_process.wait()
                logger.info("Singer pipeline executed successfully.")
            except subprocess.CalledProcessError as e:
                logger.error("Error running Singer pipeline: %s", e)
                raise


class HamiltonPipelineProtocol(t.Protocol):
    def __call__(self, *args: t.Any, **kwds: t.Any) -> t.Any: ...


class HamiltonAdapter(ExtractLoadAdapterBase[HamiltonPipelineProtocol, HamiltonAdapterConfig]):
    def _discover_pipelines(self) -> Mapping[str, HamiltonPipelineProtocol]:
        """Expose the configured adapter as a pipeline."""
        return {"main": self}

    def __call__(self, pipeline_name: str = "main", **kwargs: t.Any) -> None:
        """Run the hamilton pipeline."""
        from hamilton import driver  # pyright: ignore[reportMissingTypeStubs]

        modules = [
            self._load_module(self.package_path / script) for script in self.adapter_conf.scripts
        ]
        dr = driver.Driver({"config": self.package_conf}, *modules)
        result = dr.execute(["result"], inputs=self.adapter_conf.inputs)
        logger.info("Hamilton pipeline executed successfully: %s", result)
