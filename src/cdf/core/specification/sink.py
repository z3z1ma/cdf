import inspect
import runpy
import sys
import typing as t
from pathlib import Path
from threading import Lock

import dlt
import dynaconf
import pydantic
from sqlmesh.core.config import GatewayConfig

import cdf.core.constants as c
import cdf.core.logger as logger
from cdf.core.specification.base import InstallableRequirements, WorkspaceComponent


def _getmodulename(name: str) -> str:
    """Wraps inspect.getmodulename to ensure a module name is returned."""
    rv = inspect.getmodulename(name)
    return rv or name


class SinkSpecification(WorkspaceComponent, InstallableRequirements):
    """A sink specification."""

    ingest_config: str = "ingest"
    """The variable which holds the ingest configuration (a dlt destination)."""
    staging_config: str = "staging"
    """The variable which holds the staging configuration (a dlt destination)."""
    transform_config: str = "transform"
    """The variable which holds the transform configuration (a sqlmesh config)."""

    _exports: t.Optional[t.Dict[str, t.Any]] = None
    """Contains the exports from the sink script."""

    _folder: str = "sinks"
    """The folder where sink scripts are stored."""
    _lock: Lock = pydantic.PrivateAttr(default_factory=Lock)
    """A lock to ensure the sink is thread safe."""

    @pydantic.model_validator(mode="before")
    @classmethod
    def _clean_path_and_name(cls, config: t.Any) -> t.Any:
        if config["name"].endswith(".py"):
            if "path" not in config:
                config["path"] = config["name"]
            config["name"] = config["name"][:-3]
        else:
            if "path" not in config:
                if config["name"].endswith("_sink"):
                    config["path"] = f"{config['name']}.py"
                else:
                    config["path"] = f"{config['name']}_sink.py"
        return config

    def _run(self) -> t.Dict[str, t.Any]:
        """Run the sink script."""
        if self._exports is not None:
            return self._exports
        origpath = sys.path[:]
        sys.path = [
            str(self.workspace_path),
            *sys.path,
            str(self.workspace_path.parent),
        ]
        parts = map(_getmodulename, self.path.relative_to(self.workspace_path).parts)
        run_name = ".".join(parts)
        try:
            with self._lock:
                return runpy.run_path(
                    str(self.path),
                    run_name=run_name,
                    init_globals={
                        "__file__": str(self.path),
                        c.CDF_MAIN: run_name,
                    },
                )
        except Exception as e:
            logger.error(f"Error running sink script {self.path}: {e}")
            raise
        finally:
            sys.path = origpath

    def sink_ingest(
        self,
    ) -> t.Tuple[
        dlt.destinations.destination, t.Optional[dlt.destinations.destination]
    ]:
        """Get the ingest configuration."""
        if self._exports is None:
            self._exports = self._run()
        return self._exports[self.ingest_config], self._exports.get(self.staging_config)

    def sink_transform(self) -> GatewayConfig:
        """Get the transform configuration."""
        if self._exports is None:
            self._exports = self._run()
        return self._exports[self.transform_config]

    @classmethod
    def from_config(
        cls, name: str, root: Path, config: dynaconf.Dynaconf
    ) -> "SinkSpecification":
        config.setdefault("name", name)
        config["workspace_path"] = root
        return cls.model_validate(config, from_attributes=True)
