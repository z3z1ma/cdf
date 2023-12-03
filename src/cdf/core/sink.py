import typing as t
from dataclasses import dataclass

import dlt
import sqlmesh
from dlt.common.destination.reference import Destination
from sqlmesh.core.config.gateway import GatewayConfig

import cdf.core.constants as c
from cdf.core.transform import CDFTransformLoader


@dataclass
class sink_spec:
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

    def transform_config(self, **transform_opts: t.Any) -> sqlmesh.Config:
        """Create a transform config for this sink.

        Args:
            **transform_opts (t.Any): Additional transform options.

        Returns:
            sqlmesh.Config: The transform config.
        """
        _, _, gateway = self.unwrap()
        return sqlmesh.Config.parse_obj(
            {
                "gateways": {"cdf_managed": gateway},
                **transform_opts,
                "default_gateway": "cdf_managed",
            }
        )

    def transform_context(self, path: str, **transform_opts: t.Any) -> sqlmesh.Context:
        """Create a transform context for this sink.

        Args:
            path (str): The path to the transform.
            **transform_opts (t.Any): Additional transform options.

        Returns:
            sqlmesh.Context: The transform context.
        """
        return sqlmesh.Context(
            config=self.transform_config(**transform_opts),
            paths=[path],
            loader=CDFTransformLoader,
        )


def export_sinks(*sinks: sink_spec, scope: dict | None = None) -> None:
    """Export sinks to the callers global scope.

    Args:
        *sinks (sink_spec): The sinks to export.
        scope (dict | None, optional): The scope to export to. Defaults to globals().
    """
    if scope is None:
        import inspect

        frame = inspect.currentframe()
        if frame is not None:
            frame = frame.f_back
        if frame is not None:
            scope = frame.f_globals

    (scope or globals()).setdefault(c.CDF_SINKS, []).extend(sinks)


# Re-export for convenience
gateway = GatewayConfig
destination = dlt.destinations


__all__ = [
    "gateway",
    "destination",
    "sink_spec",
    "export_sinks",
]
