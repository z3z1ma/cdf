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

import cdf.core.models as M
from cdf.core.container import Container
from cdf.utils.files import json, load_module_from_path, yaml
from cdf.utils.general import inject_sys_path

__all__ = ["DltAdapter", "SlingAdapter", "SingerAdapter", "HamiltonAdapter"]

T = t.TypeVar("T")

logger = logging.getLogger(__name__)


@t.overload
def extract_load_adapter_factory(
    package_path: Path, container: Container, conf: M.DltAdapterConfig
) -> DltAdapter: ...


@t.overload
def extract_load_adapter_factory(
    package_path: Path, container: Container, conf: M.SingerAdapterConfig
) -> SingerAdapter: ...


@t.overload
def extract_load_adapter_factory(
    package_path: Path, container: Container, conf: M.SlingAdapterConfig
) -> SlingAdapter: ...


@t.overload
def extract_load_adapter_factory(
    package_path: Path, container: Container, conf: M.HamiltonAdapterConfig
) -> HamiltonAdapter: ...


def extract_load_adapter_factory(
    package_path: Path, container: Container, conf: M.ExtractLoadConfig
) -> ExtractLoadAdapterBase[t.Any]:
    """Factory function to create an extract-load adapter based on the provided configuration.

    Args:
        package_path (Path): The path to the package directory.
        container (Container): The dependency injection container.
        conf (M.ExtractLoadConfig): The configuration for the extract-load adapter.

    Returns:
        ExtractLoadAdapterBase[t.Any]: An instance of the appropriate extract-load adapter.

    Raises:
        ValueError: If the adapter specified in the configuration is unknown.
    """
    match conf.adapter:
        case "dlt":
            return DltAdapter(
                package_path,
                container,
                params=conf.params,
            )
        case "singer":
            return SingerAdapter(
                package_path,
                container,
                tap=conf.tap,
                target=conf.target,
                tap_config=conf.tap_config,
                target_config=conf.target_config,
                catalog=conf.catalog,
                properties=conf.properties,
                env=conf.env,
            )
        case "sling":
            return SlingAdapter(
                package_path,
                container,
                source=conf.source,
                target=conf.target,
                defaults=conf.defaults,
                streams=conf.streams,
                env=conf.env,
            )
        case "hamilton":
            return HamiltonAdapter(
                package_path,
                container,
                inputs=conf.inputs,
                scripts=list(conf.scripts),
            )
    raise ValueError(f"Unknown extract-load adapter: {conf.adapter}")  # pyright: ignore[reportUnreachable]


class ExtractLoadAdapterBase(ABC, t.Generic[T]):
    """Abstract base class for all extract-load adapters."""

    def __init__(self, package_path: Path, container: Container, **kwargs: t.Any) -> None:
        self.package_path: Path = package_path
        self.container: Container = container
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
    def __call__(self, pipeline_name: str = "main", **kwargs: t.Any) -> None:
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


@t.final
class DltAdapter(ExtractLoadAdapterBase[DltPipelineProtocol]):
    def __init__(
        self,
        package_path: Path,
        container: Container,
        params: dict[str, t.Any] | None = None,
    ) -> None:
        """
        Initialize the DltAdapter.

        Args:
            package_path (Path): The path to the package directory.
            container (Container): The dependency injection container.
            params (dict[str, t.Any], optional): Parameters for the adapter. Defaults to None.
        """
        super().__init__(package_path, container)
        self.params = params or {}

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

    def __call__(self, pipeline_name: str = "main", **kwargs: t.Any) -> None:
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
                    CustomLoaderDocProvider(provider_name, lambda: self.container.cfg)
                )
            yield
            logger.debug("Restoring DLT context")


class SlingPipelineProtocol(t.Protocol):
    def __call__(self, *args: t.Any, **kwds: t.Any) -> t.Any: ...


@t.final
class SlingAdapter(ExtractLoadAdapterBase[SlingPipelineProtocol]):
    def __init__(
        self,
        package_path: Path,
        container: Container,
        source: str,
        target: str,
        defaults: M.SlingReplicationStreamConfig,
        streams: dict[str, M.SlingReplicationStreamConfig],
        env: dict[str, t.Any] | None = None,
    ) -> None:
        """
        Initialize the SlingAdapter.

        Args:
            package_path (Path): The path to the package directory.
            container (Container): The dependency injection container.
            source (str): The source for the replication.
            target (str): The target for the replication.
            defaults (M.SlingReplicationStreamConfig): Default configuration for the replication streams.
            streams (dict[str, M.SlingReplicationStreamConfig]): Configuration for individual replication streams.
            env (dict[str, t.Any], optional): Environment variables. Defaults to None.
        """
        super().__init__(package_path, container)
        self.source = source
        self.target = target
        self.defaults = defaults
        self.streams = streams
        self.env = env or {}

    def _discover_pipelines(self) -> Mapping[str, SlingPipelineProtocol]:
        """Discover all pipelines for sling."""
        return {"main": self}

    def __call__(self, pipeline_name: str = "main", **kwargs: t.Any) -> None:
        """Run a specific pipeline."""
        logger.info(
            "Running Sling pipeline from %s to %s",
            self.source,
            self.target,
        )
        replication_conf = {
            "source": self.source,
            "target": self.target,
            "defaults": self.defaults,
            "streams": self.streams,
        }
        with tempfile.NamedTemporaryFile("w") as f:
            yaml.dump(replication_conf, f)
            f.flush()
            _ = subprocess.run(["sling", "run", "-r", f.name], check=True)


class SingerPipelineProtocol(t.Protocol):
    def __call__(self, *args: t.Any, **kwds: t.Any) -> t.Any: ...


@t.final
class SingerAdapter(ExtractLoadAdapterBase[SingerPipelineProtocol]):
    def __init__(
        self,
        package_path: Path,
        container: Container,
        tap: str,
        target: str,
        tap_config: dict[str, t.Any] | None = None,
        target_config: dict[str, t.Any] | None = None,
        catalog: dict[str, t.Any] | None = None,
        properties: dict[str, t.Any] | None = None,
        env: dict[str, str] | None = None,
    ) -> None:
        """
        Initialize the SingerAdapter.

        Args:
            package_path (Path): The path to the package directory.
            container (Container): The dependency injection container.
            tap (str): The name of the tap to use.
            target (str): The name of the target to use.
            tap_config (dict[str, t.Any], optional): Configuration for the tap. Defaults to None.
            target_config (dict[str, t.Any], optional): Configuration for the target. Defaults to None.
            catalog (dict[str, t.Any], optional): Catalog configuration. Defaults to None.
            properties (dict[str, t.Any], optional): Properties configuration. Defaults to None.
            env (dict[str, str], optional): Environment variables. Defaults to None.
        """
        super().__init__(package_path, container)
        self.tap = tap
        self.target = target
        self.tap_config = tap_config or {}
        self.target_config = target_config or {}
        self.catalog = catalog or {}
        self.properties = properties or {}
        self.env = env or {}

    def _discover_pipelines(self) -> dict[str, t.Callable[..., t.Any]]:
        """Expose adapter as callable pipeline."""
        return {"main": self}

    def __call__(self, pipeline_name: str = "main", **kwargs: t.Any) -> None:
        """Run a singer pipeline using subprocess."""
        # TODO: use pex to convert pip URIs to executable zip files
        logger.info(
            "Running Singer pipeline from %s to %s",
            self.tap,
            self.target,
        )
        with (
            tempfile.NamedTemporaryFile("w", suffix=".json") as tap_file,
            tempfile.NamedTemporaryFile("w", suffix=".json") as target_file,
        ):
            json.dump(self.tap_config, tap_file)
            tap_file.flush()
            json.dump(self.target_config, target_file)
            target_file.flush()

            tap_command = ["tap-" + self.tap, "--config", tap_file.name]
            target_command = ["target-" + self.target, "--config", target_file.name]

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


@t.final
class HamiltonAdapter(ExtractLoadAdapterBase[HamiltonPipelineProtocol]):
    def __init__(
        self,
        package_path: Path,
        container: Container,
        inputs: dict[str, t.Any] | None = None,
        scripts: list[Path | str] | None = None,
    ) -> None:
        """
        Initialize the HamiltonAdapter.

        Args:
            package_path (Path): The path to the package directory.
            container (Container): The dependency injection container.
            inputs (dict[str, t.Any], optional): Inputs for the adapter. Defaults to None.
            scripts (list[Path | str], optional): Scripts to run. Defaults to None.
        """
        super().__init__(package_path, container)
        self.inputs = inputs or {}
        self.scripts = scripts or []

    def _discover_pipelines(self) -> Mapping[str, HamiltonPipelineProtocol]:
        """Expose the configured adapter as a pipeline."""
        return {"main": self}

    def __call__(self, pipeline_name: str = "main", **kwargs: t.Any) -> None:
        """Run the hamilton pipeline."""
        from hamilton import driver  # pyright: ignore[reportMissingTypeStubs]

        modules = [self._load_module(self.package_path / script) for script in self.scripts]
        dr = driver.Driver({"config": self.package_conf}, *modules)
        result = dr.execute(["result"], inputs=self.inputs)
        logger.info("Hamilton pipeline executed successfully: %s", result)
