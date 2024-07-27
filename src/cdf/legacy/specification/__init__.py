import typing as t

from cdf.legacy.specification.notebook import NotebookSpecification
from cdf.legacy.specification.pipeline import PipelineSpecification
from cdf.legacy.specification.publisher import PublisherSpecification
from cdf.legacy.specification.script import ScriptSpecification
from cdf.legacy.specification.sink import SinkSpecification

CoreSpecification = t.Union[
    NotebookSpecification,
    PipelineSpecification,
    PublisherSpecification,
    ScriptSpecification,
    SinkSpecification,
]

__all__ = [
    "NotebookSpecification",
    "PipelineSpecification",
    "PublisherSpecification",
    "ScriptSpecification",
    "SinkSpecification",
    "CoreSpecification",
]
