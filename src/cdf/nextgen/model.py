"""Definitions for services, sources, and destinations in the workspace."""

import inspect
import sys
import typing as t
from contextlib import suppress
from dataclasses import dataclass, field
from enum import Enum
from operator import attrgetter

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
    tags: t.List[str] = field(default_factory=list)
    metadata: t.Dict[str, t.Any] = field(default_factory=dict)

    __wrappable__ = ("dependency",)

    def __post_init__(self):
        if self.sla not in ServiceLevelAgreement:
            raise ValueError(f"Invalid SLA: {self.sla}")
        if not self.name.isidentifier():
            raise ValueError(f"Invalid name: {self.name}")

    @classmethod
    def wrap(
        cls,
        dependency: injector.Dependency[T],
        name: t.Optional[str] = None,
        owner: t.Optional[str] = None,
        description: t.Optional[str] = None,
        sla: ServiceLevelAgreement = ServiceLevelAgreement.MEDIUM,
        enabled: bool = True,
        version: t.Optional[str] = None,
        tags: t.Optional[t.List[str]] = None,
        metadata: t.Optional[t.Dict[str, t.Any]] = None,
        **kwargs: t.Any,
    ):
        """Create a component with some simple heuristics to parse kwargs from the wrapped dependency."""
        tags = tags or []
        metadata = metadata or {}

        if name is None:
            for attr in ("__name__", "__qualname__", "__class__.__name__"):
                with suppress(AttributeError):
                    name = attrgetter(attr)(dependency.factory)
                    if name is not None:
                        break
            else:
                raise ValueError("Could not infer name from dependency")

        if description is None:
            description = inspect.getdoc(dependency.factory)

        if version is None and hasattr(dependency.factory, "__version__"):
            version = getattr(dependency.factory, "__version__")

        metadata.update(getattr(dependency.factory, "__metadata__", {}))
        tags.extend(getattr(dependency.factory, "__tags__", []))

        module = inspect.getmodule(dependency.factory)
        if module is not None:
            if version is None and hasattr(module, "__version__"):
                version = getattr(module, "__version__")
            metadata.update(getattr(module, "__metadata__", {}))
            tags.extend(getattr(module, "__tags__", []))

        return cls(
            name=name,
            dependency=dependency,
            owner=owner,
            description=description or "No description provided",
            sla=sla,
            enabled=enabled,
            version=version or "0.1.0",
            tags=tags,
            metadata=metadata,
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
        for fname in self.__wrappable__:
            if fname in kwargs:
                if isinstance(kwargs[fname], injector.Dependency):
                    kwargs[fname] = kwargs[fname].apply_wrappers(*decorators)
                elif callable(kwargs[fname]):
                    for decorator in decorators:
                        kwargs[fname] = decorator(kwargs[fname])
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
        metadata: t.Dict[str, t.Any]

else:

    class _ComponentProperties(t.TypedDict, total=False):
        """A dictionary of properties for component metadata."""

        name: str
        dependency: injector.Dependency[t.Any]
        owner: str
        description: str
        sla: ServiceLevelAgreement
        version: str
        metadata: t.Dict[str, t.Any]

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

    __wrappable__ = ("dependency", "integration_test")

    integration_test: t.Optional[t.Callable[[], bool]] = None

    @property
    def known_dataset(self) -> t.Optional[str]:
        """Return the dataset that the pipeline is known to load to.

        This is useful for executing GRANT and REVOKE statements in the database
        based on pipeline metadata.
        """
        dataset = getattr(self.dependency.factory, "__dataset__", None)
        if dataset is not None:
            return dataset
        module = inspect.getmodule(self.dependency.factory)
        if module is not None:
            return getattr(module, "__dataset__", None)
        return None


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
