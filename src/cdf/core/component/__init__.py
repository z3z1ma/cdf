import typing as t

from .base import Component, Entrypoint, ServiceLevelAgreement
from .operation import Operation, OperationProto
from .pipeline import DataPipeline, DataPipelineProto
from .publisher import DataPublisher, DataPublisherProto
from .service import Service, ServiceProto

__all__ = [
    "DataPipeline",
    "DataPublisher",
    "Operation",
    "Service",
    "ServiceDef",
    "DataPipelineDef",
    "DataPublisherDef",
    "OperationDef",
    "TComponent",
    "TComponent",
    "ServiceLevelAgreement",
]

ServiceDef = t.Union[
    Service,
    t.Callable[..., ServiceProto],
    t.Dict[str, t.Any],
]
DataPipelineDef = t.Union[
    DataPipeline,
    t.Callable[..., DataPipelineProto],
    t.Dict[str, t.Any],
]
DataPublisherDef = t.Union[
    DataPublisher,
    t.Callable[..., DataPublisherProto],
    t.Dict[str, t.Any],
]
OperationDef = t.Union[
    Operation,
    t.Callable[..., OperationProto],
    t.Dict[str, t.Any],
]

TComponent = t.TypeVar("TComponent", bound=t.Union[Component, Entrypoint])
TComponentDef = t.TypeVar(
    "TComponentDef",
    ServiceDef,
    DataPipelineDef,
    DataPublisherDef,
    OperationDef,
)
