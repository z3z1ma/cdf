import typing as t

from dlt.common.destination.reference import Destination
from sqlmesh.core.config import GatewayConfig

from cdf.legacy.specification.base import PythonScript
from cdf.legacy.state import with_audit


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

    @property
    def main(self) -> t.Callable[..., t.Dict[str, t.Any]]:
        """Run the sink script."""
        loader = t.cast(t.Callable[..., t.Dict[str, t.Any]], super().main)
        return with_audit(
            "load_sink",
            lambda self=self: {
                "name": self.name,
                "owner": self.owner,
                "workspace": self.workspace.name,
                "project": self.project.name,
            },
        )(loader)

    def get_ingest_config(
        self,
    ) -> t.Tuple[Destination, t.Optional[Destination]]:
        """Get the ingest configuration."""
        if self._exports is None:
            self._exports = self.main()
        return self._exports[self.ingest_config], self._exports.get(self.stage_config)

    def get_transform_config(self) -> GatewayConfig:
        """Get the transform configuration."""
        if self._exports is None:
            self._exports = self.main()
        return GatewayConfig.model_validate(self._exports[self.transform_config])

    @property
    def ingest(self) -> Destination:
        """The ingest destination."""
        return self.get_ingest_config()[0]

    @property
    def stage(self) -> t.Optional[Destination]:
        """The stage destination."""
        return self.get_ingest_config()[1]

    @property
    def transform(self) -> GatewayConfig:
        """The transform configuration."""
        return self.get_transform_config()


__all__ = ["SinkSpecification"]
