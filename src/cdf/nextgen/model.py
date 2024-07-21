"""Definitions for services, sources, and destinations in the workspace."""

import sys
import typing as t
from dataclasses import dataclass, field
from enum import Enum

import cdf.injector as injector

if t.TYPE_CHECKING:
    from dlt.common.destination import Destination as DltDestination
    from dlt.common.pipeline import LoadInfo
    from dlt.sources import DltSource

T = t.TypeVar("T")


class ServiceLevelAgreement(Enum):
    """An SLA to assign to a service or pipeline"""

    LOW = 1
    MEDIUM = 2
    HIGH = 3
    CRITICAL = 4


@dataclass(frozen=True)
class Component(t.Generic[T]):
    """Metadata for a component in the workspace."""

    name: t.Annotated[
        str, "Used for dependency injection, should be a valid python identifier"
    ]
    dependency: injector.Dependency[T]
    owner: t.Optional[str] = None
    description: str = "No description provided"
    sla: ServiceLevelAgreement = ServiceLevelAgreement.MEDIUM

    def __post_init__(self):
        if self.sla not in ServiceLevelAgreement:
            raise ValueError(f"Invalid SLA: {self.sla}")

    def __str__(self):
        return f"{self.name} ({self.sla.name})"

    def __call__(self) -> T:
        return self.dependency()


if sys.version_info >= (3, 11):

    class _ComponentProperties(t.TypedDict, t.Generic[T], total=False):
        """A dictionary of properties for component metadata."""

        name: str
        dependency: injector.Dependency[T]
        owner: str
        description: str
        sla: ServiceLevelAgreement

else:

    class _ComponentProperties(t.TypedDict, total=False):
        """A dictionary of properties for component metadata."""

        name: str
        dependency: injector.Dependency[t.Any]
        owner: str
        description: str
        sla: ServiceLevelAgreement

        def __class_getitem__(cls, _):
            return cls


Service = Component[t.Any]
"""A service that the workspace provides."""

Source = Component["DltSource"]
"""A dlt source that the workspace provides."""

Destination = Component["DltDestination"]
"""A dlt destination that the workspace provides."""

DataPipeline = Component[t.Optional["LoadInfo"]]
"""A data pipeline that the workspace provides."""

ServiceDef = t.Union[Service, _ComponentProperties[t.Any]]
SourceDef = t.Union[Source, _ComponentProperties["DltSource"]]
DestinationDef = t.Union[Destination, _ComponentProperties["DltDestination"]]
DataPipelineDef = t.Union[DataPipeline, _ComponentProperties[t.Optional["LoadInfo"]]]


TComponent = t.TypeVar("TComponent", bound=Component)
TComponentDef = t.TypeVar(
    "TComponentDef", bound=t.Union[Component, _ComponentProperties]
)

__all__ = [
    "Service",
    "Source",
    "Destination",
    "ServiceDef",
    "SourceDef",
    "DestinationDef",
]
