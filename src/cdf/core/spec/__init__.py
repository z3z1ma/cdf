"""Continuous data framework specifications"""
from cdf.core.spec._model.loader import CDFModelLoader
from cdf.core.spec.base import (
    CDF_REGISTRY,
    ComponentRegistry,
    ComponentSpecification,
    Packageable,
    Schedulable,
    SupportsComponentMetadata,
)
from cdf.core.spec.pipeline import (
    CDFResource,
    CDFSource,
    CooperativePipelineInterface,
    PipelineInterface,
    PipelineSpecification,
)
from cdf.core.spec.publisher import PublisherInterface, PublisherSpecification
from cdf.core.spec.script import ScriptInterface, ScriptSpecification
from cdf.core.spec.sink import SinkInterface, SinkSpecification, destination, gateway

__all__ = [
    "CDFSource",
    "CDFResource",
    "CooperativePipelineInterface",
    "PipelineSpecification",
    "PipelineInterface",
    "PublisherInterface",
    "PublisherSpecification",
    "SinkSpecification",
    "ScriptSpecification",
    "SinkInterface",
    "ScriptInterface",
    "CDFModelLoader",
    "CDF_REGISTRY",
    "ComponentRegistry",
    "ComponentSpecification",
    "Packageable",
    "Schedulable",
    "SupportsComponentMetadata",
    "gateway",
    "destination",
]
