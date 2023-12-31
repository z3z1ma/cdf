"""The spec classes for continuous data framework sinks."""
import typing as t

import dlt
from dlt.common.destination.reference import Destination
from sqlmesh.core.config.gateway import GatewayConfig

import cdf.core.constants as c
import cdf.core.logger as logger
from cdf.core.spec.base import ComponentSpecification

SinkCoercibleTo = t.Literal["all", "destination", "staging", "gateway"]
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


class SinkSpecification(ComponentSpecification):
    """A sink specification."""

    environment: str
    """The name of the environment. IE: dev, staging, prod."""

    _tuple: SinkInterface | None = None
    _key = c.SINKS

    @t.overload
    def __call__(self, as_: t.Literal["all"] = "all") -> SinkInterface:
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
        as_: SinkCoercibleTo = "all",
    ) -> SinkInterface | Destination | GatewayConfig | None:
        """Return the sink components"""
        if self._tuple is None:
            logger.info("Instantiating sink %s", self.name)
            dest, stg, gateway = self._main()
            for d in (dest, stg):
                if d is not None:
                    name = self.name
                    if d is stg:
                        name += "-staging"
                    dest.config_params = dest.config_params or {}
                    dest.config_params["destination_name"] = name
                    dest.config_params["environment"] = self.environment
            self._tuple = SinkInterface(dest, stg, gateway)
        if as_ == "all":
            return self._tuple
        elif as_ == "destination":
            return self._tuple.destination
        elif as_ == "staging":
            return self._tuple.staging
        elif as_ == "gateway":
            return self._tuple.gateway
        else:
            raise ValueError(
                f"Cannot coerce sink to {as_}, must be one of {t.get_args(SinkCoercibleTo)}"
            )


gateway = GatewayConfig
"""Create a SQLMesh gateway."""

destination = dlt.destinations
"""Create a DLT destination."""


__all__ = ["gateway", "destination", "SinkSpecification", "SinkInterface"]
