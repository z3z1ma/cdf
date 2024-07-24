"""Definitions for services, sources, and destinations in the workspace."""

import sys
import typing as t
from dataclasses import dataclass
from enum import Enum

from typing_extensions import Self

import cdf.injector as injector

if t.TYPE_CHECKING:
    from dlt.common.destination import Destination as DltDestination
    from dlt.common.pipeline import LoadInfo
    from dlt.sources import DltSource

T = t.TypeVar("T")


class ServiceLevelAgreement(Enum):
    """An SLA to assign to a component. Users can define the meaning of each level."""

    DEPRECATING = -1
    NONE = 0
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
    enabled: bool = True
    version: str = "0.1.0"

    __wrappable__ = ("dependency",)

    def __post_init__(self):
        if self.sla not in ServiceLevelAgreement:
            raise ValueError(f"Invalid SLA: {self.sla}")
        if not self.name.isidentifier():
            raise ValueError(f"Invalid name: {self.name}")

    @classmethod
    def with_inferred_name(
        cls,
        dependency: injector.Dependency[T],
        owner: t.Optional[str] = None,
        description: str = "No description provided",
        sla: ServiceLevelAgreement = ServiceLevelAgreement.MEDIUM,
        enabled: bool = True,
        version: str = "0.1.0",
        **kwargs: t.Any,
    ):
        """Create a component with an inferred name and description from the dependency."""
        name = getattr(dependency.factory, "__name__", None)
        if name is None:
            name = getattr(dependency.factory, "__qualname__", None)
        if name is None:
            klass = getattr(dependency.factory, "__class__", None)
            if klass is not None:
                name = getattr(klass, "__name__", None)
        if name is None:
            raise ValueError("Could not infer name from dependency")
        if description == "No description provided":
            description = getattr(dependency.factory, "__doc__", description)
        return cls(
            name=name,
            dependency=dependency,
            owner=owner,
            description=description,
            sla=sla,
            enabled=enabled,
            version=version,
            **kwargs,
        )

    def __str__(self):
        return f"{self.name} ({self.sla.name})"

    def __call__(self, *args: t.Any, **kwargs: t.Any) -> T:
        return self.dependency(*args, **kwargs)

    def apply_wrappers(
        self,
        *decorators: t.Callable[
            [t.Union[t.Callable[..., T], T]], t.Union[t.Callable[..., T], T]
        ],
    ) -> Self:
        """Apply decorators to the dependency."""
        kwargs = self.__dict__.copy()
        for field in self.__wrappable__:
            if field in kwargs:
                if isinstance(kwargs[field], injector.Dependency):
                    kwargs[field] = kwargs[field].apply_wrappers(*decorators)
                elif callable(kwargs[field]):
                    for decorator in decorators:
                        kwargs[field] = decorator(kwargs[field])
        return self.__class__(**kwargs)


if sys.version_info >= (3, 11):

    class _ComponentProperties(t.TypedDict, t.Generic[T], total=False):
        """A dictionary of properties for component metadata."""

        name: str
        dependency: injector.Dependency[T]
        owner: str
        description: str
        sla: ServiceLevelAgreement
        version: str

else:

    class _ComponentProperties(t.TypedDict, total=False):
        """A dictionary of properties for component metadata."""

        name: str
        dependency: injector.Dependency[t.Any]
        owner: str
        description: str
        sla: ServiceLevelAgreement
        version: str

        def __class_getitem__(cls, _):
            return cls


Service = Component[t.Any]
"""A service that the workspace provides. IE an API, database, requests client, etc."""

Source = Component["DltSource"]
"""A dlt source which we can extract data from."""

Destination = Component["DltDestination"]
"""A dlt destination which we can load data into."""


@dataclass(frozen=True)
class DataPipeline(Component[t.Optional["LoadInfo"]]):
    """A data pipeline which loads data from a source to a destination."""

    integration_test: t.Optional[t.Callable[[], bool]] = None

    __wrappable__ = ("dependency", "integration_test")


@dataclass(frozen=True)
class DataPublisher(Component[t.Any]):
    """A data publisher which pushes data to an operational system."""

    pre_check: t.Optional[t.Callable[[], bool]] = None

    __wrappable__ = ("dependency", "pre_check")


Operation = Component[int]
"""A generic callable that returns an exit code."""

ServiceDef = t.Union[Service, _ComponentProperties[t.Any]]
SourceDef = t.Union[Source, _ComponentProperties["DltSource"]]
DestinationDef = t.Union[Destination, _ComponentProperties["DltDestination"]]
DataPipelineDef = t.Union[DataPipeline, _ComponentProperties[t.Optional["LoadInfo"]]]
DataPublisherDef = t.Union[DataPublisher, _ComponentProperties[t.Any]]
OperationDef = t.Union[Operation, _ComponentProperties[int]]

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
    "DataPipeline",
    "DataPipelineDef",
    "DataPublisher",
    "DataPublisherDef",
    "Operation",
    "OperationDef",
    "ServiceLevelAgreement",
    "Component",
    "TComponent",
]
