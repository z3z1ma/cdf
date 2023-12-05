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
from cdf.core.publisher import Payload, export_publishers, publisher_spec
from cdf.core.sink import destination, export_sinks, gateway, sink_spec
from cdf.core.source import (
    CDFResource,
    CDFSource,
    PipeGen,
    export_pipelines,
    pipeline_spec,
)
from cdf.core.workspace import Project, Workspace

inject_config = dlt.config.value
"""A sentinel value used to mark a value as needing to be injected from a config provider."""
inject_secret = dlt.secrets.value
"""A sentinel value used to mark a value as needing to be injected from a secret provider."""

incremental = dlt.sources.incremental
"""Add incremental support to a resource."""

__all__ = [
    "CDFSource",
    "CDFResource",
    "Project",
    "Workspace",
    "pipeline_spec",
    "export_pipelines",
    "PipeGen",
    "publisher_spec",
    "export_publishers",
    "Payload",
    "sink_spec",
    "export_sinks",
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
