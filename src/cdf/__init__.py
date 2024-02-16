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
from dlt import config
from dlt import destinations as destination
from dlt import secrets
from dlt.common.configuration import with_config
from sqlmesh.core.config import GatewayConfig as gateway
from sqlmesh.core.config import parse_connection_config

from cdf.core.sandbox import run
from cdf.core.workspace import find_nearest, get_gateway

inject_config = config.value
inject_secret = secrets.value

__all__ = [
    "find_nearest",
    "run",
    "get_gateway",
    "with_config",
    "config",
    "secrets",
    "inject_config",
    "inject_secret",
    "destination",
    "gateway",
    "parse_connection_config",
]
