"""CDF - Continuous Data Framework

CDF is a framework for managing data end to end. It can be though of as a wrapper on top of
2 best-in-class open source projects: sqlmesh and dlt. It provides a way to manage data
from ingestion to transformation to publishing. It gives you a unified pane of glass with
an opnionated project structure supporting both multi-workspace and single-workspace
layouts allowing it to scale from small to large projects. It provides opinionated features
that augment dlt and sqlmesh including automated virtual environment management, automated
discoverability of pipelines and publishers, automated configuration management, and
more.
"""
import dlt
from dlt import pipeline as pipeline
from dlt.common.configuration import configspec as configspec
from dlt.common.configuration import with_config as with_config
from dlt.sources.helpers.requests import Client as session

import cdf.core.logger as logger
from cdf.core.spec import (
    CooperativePipelineInterface,
    NotebookSpecification,
    PipelineSpecification,
    PublisherInterface,
    PublisherSpecification,
    SinkInterface,
    SinkSpecification,
    StagingRuleset,
    StagingSpecification,
    SupportsComponentMetadata,
    destination,
    gateway,
)
from cdf.core.workspace import Project, Workspace

# Re-export most commonly accessed dlt symbols
CDFSource = dlt.sources.DltSource
CDFResource = dlt.sources.DltResource
inject_config = dlt.config.value
inject_secret = dlt.secrets.value
incremental = dlt.sources.incremental


__all__ = [
    "CDFSource",
    "CDFResource",
    "Project",
    "Workspace",
    "PipelineSpecification",
    "CooperativePipelineInterface",
    "NotebookSpecification",
    "PublisherSpecification",
    "PublisherInterface",
    "SinkSpecification",
    "SinkInterface",
    "StagingSpecification",
    "StagingRuleset",
    "SupportsComponentMetadata",
    "logger",
    "with_config",
    "pipeline",
    "inject_config",
    "inject_secret",
    "configspec",
    "destination",
    "gateway",
    "session",
    "incremental",
]
