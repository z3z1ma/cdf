import typing as t
from dataclasses import dataclass

import dlt
import sqlmesh
from dlt.common.destination.reference import Destination
from sqlmesh.core.config.gateway import GatewayConfig

from cdf.core.component._model.loader import CDFTransformLoader


@dataclass
class sink_spec:
    """A sink specification."""

    name: str
    """The name of the sink."""
    environment: str
    """The name of the environment. IE: dev, staging, prod."""
    destination: t.Callable[..., Destination] | Destination | None = None
    """A callable that returns a destination for ingesting data."""
    staging: t.Callable[..., Destination] | Destination | None = None
    """A callable that returns a destination for staging data."""
    gateway: t.Callable[..., GatewayConfig] | GatewayConfig | None = None
    """The gateway configuration."""

    def __post_init__(self) -> None:
        if not any([self.destination, self.staging, self.gateway]):
            raise ValueError(
                f"You must provide at least one of ingest, staging, or gateway in the {self.name} sink."
            )

    def unwrap(
        self,
    ) -> t.Tuple[Destination | None, Destination | None, GatewayConfig | None]:
        """Unwrap the sink into ingest, staging, and gateway"""

        destination = (
            self.destination() if callable(self.destination) else self.destination
        )
        staging = self.staging() if callable(self.staging) else self.staging
        gateway = self.gateway() if callable(self.gateway) else self.gateway

        if destination is not None:
            destination.config_params = destination.config_params or {}
            destination.config_params["destination_name"] = self.name
            destination.config_params["environment"] = self.environment

        if staging is not None:
            staging.config_params = staging.config_params or {}
            staging.config_params["destination_name"] = self.name + "-staging"
            staging.config_params["environment"] = self.environment

        return destination, staging, gateway

    def transform_config(self, project: str, **transform_opts: t.Any) -> sqlmesh.Config:
        """Create a transform config for this sink.

        Args:
            **transform_opts (t.Any): Additional transform options.

        Returns:
            sqlmesh.Config: The transform config.
        """
        _, _, gateway = self.unwrap()
        return sqlmesh.Config.parse_obj(
            {
                **transform_opts,
                "gateways": {self.name: gateway},
                "default_gateway": self.name,
                "project": project,
            }
        )

    def transform_context(
        self, path: str, project: str, load: bool = True, **transform_opts: t.Any
    ) -> sqlmesh.Context:
        """Create a transform context for this sink.

        Args:
            path (str): The path to the transform.
            **transform_opts (t.Any): Additional transform options.

        Returns:
            sqlmesh.Context: The transform context.
        """
        return sqlmesh.Context(
            config=self.transform_config(project, **transform_opts),
            paths=[path],
            loader=CDFTransformLoader,
            load=load,
        )


gateway = GatewayConfig
"""Create a SQLMesh gateway."""

destination = dlt.destinations
"""Create a DLT destination."""


__all__ = ["gateway", "destination", "sink_spec"]
