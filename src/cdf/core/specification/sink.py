import typing as t

from dlt.common.destination.reference import Destination
from sqlmesh.core.config import GatewayConfig

from cdf.core.specification.base import PythonScript


class SinkSpecification(PythonScript):
    """A sink specification."""

    ingest_config: str = "ingest"
    """The variable which holds the ingest configuration (a dlt destination)."""
    stage_config: str = "stage"
    """The variable which holds the staging configuration (a dlt destination)."""
    transform_config: str = "transform"
    """The variable which holds the transform configuration (a sqlmesh config)."""

    _exports: t.Optional[t.Dict[str, t.Any]] = None
    """Caches the exports from the sink script."""

    _folder: str = "sinks"
    """The folder where sink scripts are stored."""

    def get_ingest_config(
        self,
    ) -> t.Tuple[Destination, t.Optional[Destination]]:
        """Get the ingest configuration."""
        if self._exports is None:
            self._exports = self()
        return self._exports[self.ingest_config], self._exports.get(self.stage_config)

    def get_transform_config(self) -> GatewayConfig:
        """Get the transform configuration."""
        if self._exports is None:
            self._exports = self()
        return self._exports[self.transform_config]


__all__ = ["SinkSpecification"]
