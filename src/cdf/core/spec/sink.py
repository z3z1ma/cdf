"""The spec classes for continuous data framework sinks."""
import typing as t

import dlt
from dlt.common.destination.reference import Destination
from sqlmesh.core.config.gateway import GatewayConfig

import cdf.core.constants as c
from cdf.core.spec.base import ComponentSpecification


class SinkInterface(t.NamedTuple):
    """A tuple of a destination, staging destination, and gateway which represent a logical sink."""

    destination: Destination | None
    staging: Destination | None
    gateway: GatewayConfig | None


class SinkSpecification(ComponentSpecification):
    """A sink specification."""

    environment: str
    """The name of the environment. IE: dev, staging, prod."""

    _tuple: SinkInterface | None = None
    _key = c.SINKS

    @property
    def destination(self) -> Destination | None:
        """The destination for ingesting data."""
        if self._tuple is None:
            self._tuple = t.cast(SinkInterface, self._main())
        dest = self._tuple[0]
        if dest is not None:
            # Ensure name is derived from the spec
            dest.config_params = dest.config_params or {}
            dest.config_params["destination_name"] = self.name
            dest.config_params["environment"] = self.environment
        return dest

    @property
    def staging(self) -> Destination | None:
        """The destination for staging data."""
        if self._tuple is None:
            self._tuple = t.cast(SinkInterface, self._main())
        stage = self._tuple[1]
        if stage is not None:
            # Ensure name is derived from the spec
            stage.config_params = stage.config_params or {}
            stage.config_params["destination_name"] = self.name + "-staging"
            stage.config_params["environment"] = self.environment
        return stage

    @property
    def gateway(self) -> GatewayConfig | None:
        """The gateway configuration."""
        if self._tuple is None:
            self._tuple = t.cast(SinkInterface, self._main())
        return self._tuple[2]

    def __call__(self) -> SinkInterface:
        """Return the sink components"""
        return SinkInterface(self.destination, self.staging, self.gateway)


gateway = GatewayConfig
"""Create a SQLMesh gateway."""

destination = dlt.destinations
"""Create a DLT destination."""


__all__ = ["gateway", "destination", "SinkSpecification", "SinkInterface"]
