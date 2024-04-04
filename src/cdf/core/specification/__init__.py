import typing as t

from cdf.core.specification.notebook import NotebookSpecification
from cdf.core.specification.pipeline import PipelineSpecification
from cdf.core.specification.publisher import PublisherSpecification
from cdf.core.specification.script import ScriptSpecification
from cdf.core.specification.sink import SinkSpecification

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
