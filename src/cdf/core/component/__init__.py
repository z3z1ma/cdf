import typing as t

from .base import Component, Entrypoint, ServiceLevelAgreement
from .operation import Operation
from .pipeline import DataPipeline
from .publisher import DataPublisher
from .service import Service

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

ServiceDef = t.Union[Service, t.Dict[str, t.Any]]
DataPipelineDef = t.Union[DataPipeline, t.Dict[str, t.Any]]
DataPublisherDef = t.Union[DataPublisher, t.Dict[str, t.Any]]
OperationDef = t.Union[Operation, t.Dict[str, t.Any]]

TComponent = t.TypeVar("TComponent", bound=t.Union[Component, Entrypoint])
TComponentDef = t.TypeVar(
    "TComponentDef",
    ServiceDef,
    DataPipelineDef,
    DataPublisherDef,
    OperationDef,
)
