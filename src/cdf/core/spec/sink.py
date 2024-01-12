"""The spec classes for continuous data framework sinks."""
import typing as t

import dlt
from dlt.common.destination.reference import Destination
from sqlmesh.core.config.gateway import GatewayConfig

import cdf.core.constants as c
from cdf.core.spec.base import ComponentSpecification, Executable

_AsTypes = t.Literal["destination", "staging", "gateway"]
"""A sink is a callable which can be coerced to any of the above parts at calltime as a convenience"""


class SinkInterface(t.NamedTuple):
    """
    A tuple of a destination, staging destination, and gateway.

    A sink represents 1 physical destination, typically a data lake or data warehouse. There are
    two systems, namely DLT and SQLMesh which must interface with a single data store through their own
    respective configurations. Sinks allow us to group these configs and represent them as a single logical
    unit throughout the rest of CDF.
    """

    destination: Destination | None
    staging: Destination | None
    gateway: GatewayConfig | None


class SinkSpecification(ComponentSpecification, Executable):
    """A sink specification."""

    environment: str
    """The name of the environment. IE: dev, staging, prod."""

    _tuple: SinkInterface | None = None
    _key = c.SINKS

    @t.overload
    def __call__(self, as_: None = None) -> SinkInterface:
        ...

    @t.overload
    def __call__(
        self, as_: t.Literal["destination"] = "destination"
    ) -> Destination | None:
        ...

    @t.overload
    def __call__(self, as_: t.Literal["staging"] = "staging") -> Destination | None:
        ...

    @t.overload
    def __call__(self, as_: t.Literal["gateway"] = "gateway") -> GatewayConfig | None:
        ...

    def __call__(
        self,
        as_: _AsTypes | None = None,
    ) -> SinkInterface | Destination | GatewayConfig | None:
        """Return the sink components"""
        if self._tuple is None:
            destination, staging, gateway = self.main()
            for part in (destination, staging):
                if part is not None:
                    name = self.name
                    if part is staging:
                        name += "-staging"
                    part.config_params = part.config_params or {}
                    part.config_params["destination_name"] = name
                    part.config_params["environment"] = self.environment
            self._tuple = SinkInterface(destination, staging, gateway)
        if as_ is None:
            return self._tuple
        elif as_ == "destination":
            return self._tuple.destination
        elif as_ == "staging":
            return self._tuple.staging
        elif as_ == "gateway":
            return self._tuple.gateway
        else:
            raise ValueError(
                f"Cannot coerce sink to {as_}, must be one of {t.get_args(_AsTypes)}"
            )


gateway = GatewayConfig
"""Create a SQLMesh gateway."""

destination = dlt.destinations
"""Create a DLT destination."""


__all__ = ["gateway", "destination", "SinkSpecification", "SinkInterface"]
