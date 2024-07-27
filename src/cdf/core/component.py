"""Definitions for services, sources, and destinations in the workspace."""

import inspect
import sys
import typing as t
from contextlib import suppress
from dataclasses import dataclass, field
from enum import Enum
from operator import attrgetter

from typing_extensions import Self

import cdf.core.injector as injector

if t.TYPE_CHECKING:
    from dlt.common.destination import Destination as DltDestination
    from dlt.common.pipeline import LoadInfo
    from dlt.sources import DltSource

T = t.TypeVar("T")

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
    """The name of the component. This is used for dependency injection and should be a valid python identifier."""
    dependency: injector.Dependency[T]
    """The dependency for the component. This is what is injected into the workspace or exposed as a entrypoint."""
    owner: t.Optional[str] = None
    """The owner of the component. Useful for tracking who to contact for issues."""
    description: str = "No description provided"
    """A description of the component."""
    sla: ServiceLevelAgreement = ServiceLevelAgreement.MEDIUM
    """The SLA for the component."""
    enabled: bool = True
    """Whether the component is enabled or disabled. Disabled components are not loaded."""
    version: str = "0.1.0"
    """A semantic version for the component. Can signal breaking changes to dependents."""
    tags: t.List[str] = field(default_factory=list)
    """Tags to categorize the component."""
    metadata: t.Dict[str, t.Any] = field(default_factory=dict)
    """Additional metadata for the component. Useful for custom integrations."""

    __wrappable__ = ("dependency",)
    """Attributes that can be wrapped with decorators.

    Generally this means we will apply dependency injection decorators to these attributes.
    """

    __entrypoint__ = False
    """Indicates if the component is an entrypoint.

    Entrypoints are exposed in CLI commands and thus have additional constraints.
    """

    def __post_init__(self):
        if self.sla not in ServiceLevelAgreement:
            raise ValueError(f"Invalid SLA: {self.sla}")
        if not self.name.isidentifier():
            raise ValueError(f"Invalid name: {self.name}")
        if self.__entrypoint__:
            if not self.dependency.lifecycle.is_instance:
                raise ValueError(f"{self.__class__.__name__} must be an instance")
            if not callable(self.dependency.factory):
                raise ValueError(
                    f"{self.__class__.__name__}'s dependency must be a callable"
                )

    @classmethod
    def wrap(
        cls,
        dependency: t.Union[injector.Dependency[T], t.Callable[..., T], T],
        name: t.Optional[str] = None,
        owner: t.Optional[str] = None,
        description: t.Optional[str] = None,
        sla: t.Optional[ServiceLevelAgreement] = None,
        enabled: t.Optional[bool] = None,
        version: t.Optional[str] = None,
        tags: t.Optional[t.List[str]] = None,
        metadata: t.Optional[t.Dict[str, t.Any]] = None,
        **kwargs: t.Any,
    ):
        """Create a component with some simple heuristics to parse kwargs from the wrapped dependency."""
        tags = tags or []
        metadata = metadata or {}

        # Marshal dependency into a Dependency instance
        if not isinstance(dependency, injector.Dependency):
            if cls.__entrypoint__:
                dependency = injector.Dependency.instance(dependency)
            else:
                if callable(dependency):
                    dependency = injector.Dependency.singleton(dependency)
                else:
                    dependency = injector.Dependency.instance(dependency)

        # Infer name from dependency if not provided
        if name is None:
            for attr in ("__name__", "__qualname__", "__class__.__name__"):
                with suppress(AttributeError):
                    name = attrgetter(attr)(dependency.factory)
                    if name is not None:
                        break
            else:
                raise ValueError(
                    "Could not infer name from dependency and no name provided"
                )

        # Infer description
        if description is None:
            description = inspect.getdoc(dependency.factory)

        # Get the module of the factory
        _module = inspect.getmodule(dependency.factory)

        # Helper function to get attributes from factory/module
        def _get(
            *attrs: str, callback: t.Optional[t.Callable[[t.Any], t.Any]] = None
        ) -> t.Optional[t.Any]:
            for attr in attrs:
                with suppress(AttributeError):
                    v = getattr(dependency.factory, attr)
                    if callback:
                        callback(v)
                    else:
                        return v
                if _module is not None:
                    with suppress(AttributeError):
                        v = getattr(_module, attr)
                        if callback:
                            callback(v)
                        else:
                            return v

        # Infer version, enabled, owner, and sla from factory/module
        if version is None:
            if (version := _get("__version__")) is None:
                version = "0.1.0"

        if enabled is None:
            if (enabled := _get("__enabled__")) is None:
                enabled = True

        if sla is None:
            sla = _get("__sla__", "__sla_level__")
            if isinstance(sla, int):
                sla = ServiceLevelAgreement(sla)
            elif isinstance(sla, str):
                sla = ServiceLevelAgreement[sla.upper()]

        if owner is None:
            owner = _get("__owner__")

        # Merge tags and metadata from factory/module
        _get("__tags__", callback=tags.extend)
        _get("__metadata__", callback=metadata.update)

        return cls(
            name=name,
            dependency=dependency,
            owner=owner,
            description=description or "No description provided",
            sla=sla or ServiceLevelAgreement.MEDIUM,
            enabled=enabled,
            version=version,
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

    __entrypoint__ = True
    __wrappable__ = ("dependency", "integration_test")

    integration_test: t.Optional[t.Callable[[], bool]] = None
    """A function to test the pipeline in an integration environment"""

    @property
    def associated_datasets(self) -> t.List[str]:
        """Return the datasets that the pipeline is known to load to.

        This is useful for executing GRANT and REVOKE statements in the database
        based on pipeline metadata.
        """
        dataset = getattr(self.dependency.factory, "__dataset__", None)
        if dataset is not None:
            return dataset.split(";")
        module = inspect.getmodule(self.dependency.factory)
        if module is not None:
            dataset = getattr(module, "__dataset__", None)
            if dataset is not None:
                return dataset.split(";")
        return []


def _continue() -> bool:
    """A default preflight check which always returns True."""
    return True


@dataclass(frozen=True)
class DataPublisher(Component[t.Any]):
    """A data publisher which pushes data to an operational system."""

    __entrypoint__ = True
    __wrappable__ = ("dependency", "preflight_check")

    preflight_check: t.Callable[[], bool] = _continue
    """A user defined function to check if the data publisher is able to publish data"""

    @property
    def associated_datasets(self) -> t.List[str]:
        """Return the datasets that the pipeline is known to load from.

        This is useful for validating that the data publisher is accessing
        non-deprecated datasets which are not stale.
        """
        dataset = getattr(self.dependency.factory, "__dataset__", None)
        if dataset is not None:
            return dataset.split(";")
        module = inspect.getmodule(self.dependency.factory)
        if module is not None:
            dataset = getattr(module, "__dataset__", None)
            if dataset is not None:
                return dataset.split(";")
        return []


class Operation(Component[int]):
    """A generic callable that returns an exit code."""

    __entrypoint__ = True


ServiceDef = t.Union[
    Service,
    _ComponentProperties[t.Any],
    t.Callable[..., t.Any],
]

SourceDef = t.Union[
    Source,
    _ComponentProperties["DltSource"],
    t.Callable[..., "DltSource"],
]

DestinationDef = t.Union[
    Destination,
    _ComponentProperties["DltDestination"],
    t.Callable[..., "DltDestination"],
]

DataPipelineDef = t.Union[
    DataPipeline,
    _ComponentProperties[t.Optional["LoadInfo"]],
    t.Callable[..., t.Optional["LoadInfo"]],
]

DataPublisherDef = t.Union[
    DataPublisher,
    _ComponentProperties[t.Any],
    t.Callable[..., t.Any],
]

OperationDef = t.Union[
    Operation,
    _ComponentProperties[int],
    t.Callable[..., int],
]

TComponent = t.TypeVar(
    "TComponent",
    bound=Component,
)

TComponentDef = t.TypeVar(
    "TComponentDef",
    bound=t.Union[Component, _ComponentProperties, t.Callable[..., t.Any]],
)
