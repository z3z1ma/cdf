import typing as t
from dataclasses import dataclass
from enum import Enum

import cdf.injector as injector

if t.TYPE_CHECKING:
    import dlt


class ServiceLevelAgreement(Enum):
    """An SLA to assign to a service or pipeline"""

    LOW = 1
    MEDIUM = 2
    HIGH = 3
    CRITICAL = 4


@dataclass
class Service:
    """A service that the workspace provides."""

    name: injector.DependencyKey
    dependency: injector.Dependency[t.Any]
    owner: str
    description: str = "No description provided"
    sla: ServiceLevelAgreement = ServiceLevelAgreement.MEDIUM

    def __post_init__(self):
        if self.sla not in ServiceLevelAgreement:
            raise ValueError(f"Invalid SLA: {self.sla}")

    def __str__(self):
        return f"{self.name} ({self.sla.name})"

    def __call__(self) -> t.Any:
        return self.dependency()


class _Service(t.TypedDict, total=False):
    """A service type hint."""

    name: injector.DependencyKey
    dependency: injector.Dependency[t.Any]
    owner: str
    description: str
    sla: ServiceLevelAgreement


ServiceDef = t.Union[Service, _Service]


@dataclass
class Source:
    """A dlt source that the workspace provides."""

    name: str
    dependency: injector.Dependency["dlt.sources.DltSource"]
    owner: str
    description: str = "No description provided"
    sla: ServiceLevelAgreement = ServiceLevelAgreement.MEDIUM

    def __post_init__(self):
        if self.sla not in ServiceLevelAgreement:
            raise ValueError(f"Invalid SLA: {self.sla}")

    def __str__(self):
        return f"{self.name} ({self.sla.name})"

    def __call__(self) -> "dlt.sources.DltSource":
        return self.dependency()


class _Source(t.TypedDict, total=False):
    """A source type hint."""

    name: str
    dependency: injector.Dependency["dlt.sources.DltSource"]
    owner: str
    description: str
    sla: ServiceLevelAgreement


SourceDef = t.Union[Source, _Source]
