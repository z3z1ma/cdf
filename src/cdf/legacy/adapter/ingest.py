# pyright: reportImportCycles=false, reportUnknownMemberType=false
"""Extract-load adapters for different ETL frameworks."""

from __future__ import annotations

import inspect
import logging
import os
import runpy
import subprocess  # nosec
import tempfile
import typing as t
from abc import ABC, abstractmethod
from collections.abc import Mapping
from contextlib import contextmanager, suppress
from fnmatch import fnmatch
from pathlib import Path
from types import ModuleType

import cdf.legacy.constants as c
import cdf.legacy.interface as I
from cdf.commons.file import json, load_module_from_path, yaml
from cdf.commons.pyutils import inject_sys_path
from cdf.legacy.configuration import ConfigurationLoader
from cdf.legacy.container import Container

__all__ = ["DltAdapter", "SlingAdapter", "SingerAdapter", "HamiltonAdapter"]

T = t.TypeVar("T")

logger = logging.getLogger(__name__)


@t.overload
def ingest_adapter_factory(
    package_path: Path, container: Container, conf: I.DltAdapterConfig
) -> DltAdapter: ...


@t.overload
def ingest_adapter_factory(
    package_path: Path, container: Container, conf: I.SingerAdapterConfig
) -> SingerAdapter: ...


@t.overload
def ingest_adapter_factory(
    package_path: Path, container: Container, conf: I.SlingAdapterConfig
) -> SlingAdapter: ...


@t.overload
def ingest_adapter_factory(
    package_path: Path, container: Container, conf: I.HamiltonAdapterConfig
) -> HamiltonAdapter: ...


def ingest_adapter_factory(
    package_path: Path, container: Container, conf: I.ExtractLoadConfig
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
                tap_name=conf.tap_name,
                target_name=conf.target_name,
                tap_requirement=conf.tap_requirement,
                target_requirement=conf.target_requirement,
                tap_config=conf.tap_config,
                target_config=conf.target_config,
                tap_catalog=conf.tap_catalog,
                tap_supports_state=conf.tap_supports_state,
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

    # TODO: Create a central schema representation we can use across dlt, sling, singer, hamilton, etc.
    def schema(self) -> t.Any:
        """Return a schema for the package."""
        raise NotImplementedError("Schema generation not implemented for this adapter")

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

    def _run_scripts(self, pattern: str, /, **global_context: t.Any) -> dict[str, t.Any]:
        """Run scripts matching the given pattern in the package dir."""
        merged_output: dict[str, t.Any] = {}
        scripts = sorted(self.package_path.glob(f"{pattern}.py"))
        for script in scripts:
            logger.info("Running after script: %s", script)
            merged_output.update(
                runpy.run_path(
                    str(script),
                    init_globals={
                        **global_context,
                        "C": self.container,
                    },
                )
            )
        return merged_output


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
        main_script = self.package_path / c.DLT_ENTRYPOINT
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
        defaults: I.SlingReplicationStreamConfig,
        streams: dict[str, I.SlingReplicationStreamConfig],
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
            "defaults": self.defaults.model_dump(by_alias=True, exclude_none=True),
            "streams": {
                k: v.model_dump(by_alias=True, exclude_none=True) for k, v in self.streams.items()
            },
        }
        logger.info("Running before scripts")
        _ = self._run_scripts("before_*", replication_conf=replication_conf)
        with tempfile.NamedTemporaryFile("w") as f:
            yaml.dump(replication_conf, f)
            f.flush()
            cmd = ["sling", "run", "-r", f.name]
            process = subprocess.run(cmd)
        if process.returncode > 0:
            _ = self._run_scripts("after_error_*", replication_conf=replication_conf)
            raise subprocess.CalledProcessError(process.returncode, cmd)
        else:
            _ = self._run_scripts("after_success_*", replication_conf=replication_conf)


class SingerPipelineProtocol(t.Protocol):
    def __call__(self, *args: t.Any, **kwds: t.Any) -> t.Any: ...


@t.final
class SingerAdapter(ExtractLoadAdapterBase[SingerPipelineProtocol]):
    def __init__(
        self,
        package_path: Path,
        container: Container,
        tap_name: str,
        target_name: str,  # TODO: how can we pipe tap data to dlt target intuitively (dlt:<dest name>?)
        tap_requirement: str | None = None,
        target_requirement: str | None = None,
        tap_config: dict[str, t.Any] | Path | str | None = None,
        target_config: dict[str, t.Any] | Path | str | None = None,
        tap_catalog: dict[str, t.Any] | Path | str | None = None,
        tap_supports_state: bool = True,
        env: dict[str, str] | None = None,
    ) -> None:
        """
        Initialize the SingerAdapter.

        Args:
            package_path (Path): The path to the package directory.
            container (Container): The dependency injection container.
            tap_name (str): The name of the tap to use.
            target_name (str): The name of the target to use.
            tap_requirement (str, optional): Pip dependency string for the tap. Defaults to None.
            tap_config (dict[str, t.Any], optional): Configuration for the tap. Defaults to None.
            tap_catalog (dict[str, t.Any], optional): Catalog configuration. Defaults to None.
            target_requirement (str, optional): Pip dependency string for the target. Defaults to None.
            target_config (dict[str, t.Any], optional): Configuration for the target. Defaults to None.
            env (dict[str, str], optional): Environment variables to inject in subprocesses. Defaults to None.
        """
        super().__init__(package_path, container)

        def _resolve_pathlike(p: Path | str) -> Path:
            p = Path(p)
            if not p.is_absolute():
                p = package_path / p
            return p

        def _load(p: Path | str) -> dict[str, t.Any]:
            """Load configuration from a path."""
            return ConfigurationLoader(
                _resolve_pathlike(p), include_envvars=False, context="package"
            ).load()

        if isinstance(tap_config, (Path, str)):
            tap_config = _load(tap_config)
        if isinstance(target_config, (Path, str)):
            target_config = _load(target_config)
        if isinstance(tap_catalog, (Path, str)):
            tap_catalog = _resolve_pathlike(tap_catalog)

        self.tap_name = tap_name
        self.target_name = target_name
        self.tap_requirement = tap_requirement or tap_name
        self.tap_config = tap_config or {}
        self.tap_catalog = tap_catalog
        self.target_requirement = target_requirement or target_name
        self.target_config = target_config or {}
        self.tap_supports_state = tap_supports_state
        self.env = env or {}

    def _discover_pipelines(self) -> dict[str, t.Callable[..., t.Any]]:
        """Expose adapter as callable pipeline."""
        return {"main": self}

    def __call__(self, pipeline_name: str = "main", **kwargs: t.Any) -> None:
        """Run a singer pipeline using subprocess."""
        logger.info(
            "Running Singer pipeline from %s to %s",
            self.tap_name,
            self.target_name,
        )
        tap_cmd = ["uvx", "--from", self.tap_requirement, self.tap_name]
        tgt_cmd = ["uvx", "--from", self.target_requirement, self.target_name]
        state: dict[str, t.Any] = self.container["cdf_state"]
        _ = self._run_scripts("before_*", tap_cmd=tap_cmd, tgt_cmd=tgt_cmd, state=state)
        # TODO: handle notion of catalog, and "capabilities" with as little fuss as possible
        with (
            tempfile.NamedTemporaryFile("w", suffix=".json") as tap_conf_f,
            tempfile.NamedTemporaryFile("w", suffix=".json") as tgt_conf_f,
            tempfile.NamedTemporaryFile("w", suffix=".json") as tap_state_f,
        ):
            json.dump(dict(self.tap_config or {}), tap_conf_f)
            tap_conf_f.flush()
            json.dump(dict(self.target_config or {}), tgt_conf_f)
            tgt_conf_f.flush()
            tap_cmd.extend(["--config", tap_conf_f.name])
            tgt_cmd.extend(["--config", tgt_conf_f.name])
            if self.tap_supports_state:
                json.dump(dict(state), tap_state_f)
                tap_state_f.flush()
                tap_cmd.extend(["--state", tap_state_f.name])
            try:
                last_state_message = None
                with (
                    subprocess.Popen(  # nosec
                        tap_cmd,
                        stdout=subprocess.PIPE,
                        env={**os.environ, **self.env},
                    ) as tap_proc,
                    subprocess.Popen(  # nosec
                        tgt_cmd,
                        stdin=tap_proc.stdout,
                        stdout=subprocess.PIPE,
                        env={**os.environ, **self.env},
                    ) as tgt_proc,
                ):
                    assert tgt_proc.stdout
                    for line in iter(tgt_proc.stdout):
                        with suppress(json.JSONDecodeError):
                            last_state_message = json.loads(line.decode())
                if tap_proc.returncode > 0:
                    raise subprocess.CalledProcessError(tap_proc.returncode, tgt_cmd)
                if tgt_proc.returncode > 0:
                    raise subprocess.CalledProcessError(tgt_proc.returncode, tgt_cmd)
                if last_state_message:
                    state.update(last_state_message)
                logger.info("Singer pipeline executed successfully.")
            except subprocess.CalledProcessError as e:
                logger.error("Error running Singer pipeline: %s", e)
                _ = self._run_scripts(
                    "after_error_*", tap_cmd=tap_cmd, tgt_cmd=tgt_cmd, state=state
                )
                raise
            _ = self._run_scripts("after_success_*", tap_cmd=tap_cmd, tgt_cmd=tgt_cmd, state=state)


# WIP: The meltano adapter should allow us to basically actuate a meltano.yml either by itself
# or embedded in package configuration
class MeltanoPipelineProtocol(t.Protocol):
    def __call__(self, *args: t.Any, **kwds: t.Any) -> t.Any: ...


@t.final
class MeltanoAdapter(ExtractLoadAdapterBase[MeltanoPipelineProtocol]):
    def __init__(
        self,
        package_path: Path,
        container: Container,
        tap_name: str,
        target_name: str,
    ) -> None:
        super().__init__(package_path, container)
        self.tap_name = tap_name
        self.target_name = target_name

    def _discover_pipelines(self) -> dict[str, t.Callable[..., t.Any]]:
        """Expose adapter as callable pipeline."""
        return {"main": self}

    def __call__(self, pipeline_name: str = "main", **kwargs: t.Any) -> None:
        cmd = ["uvx", "meltano", "run", self.tap_name, self.target_name]
        _ = subprocess.run(cmd, check=True, cwd=self.package_path)


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
