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
import os
import typing as t
from types import SimpleNamespace

import dlt.sources.helpers.requests as requests
from dlt import config
from dlt import destinations as destination
from dlt import secrets
from dlt.common.configuration import with_config
from dlt.sources import incremental
from sqlmesh.core.config import ConnectionConfig
from sqlmesh.core.config import GatewayConfig as gateway
from sqlmesh.core.config import parse_connection_config as _parse_connection_config

import cdf.core.logger as logger
from cdf.core.context import active_workspace
from cdf.core.context import current_spec as _current_spec
from cdf.core.sandbox import run
from cdf.core.workspace import Workspace, find_nearest, get_gateway, to_context

inject_config = config.value
inject_secret = secrets.value

session = requests.Client


def current_spec() -> SimpleNamespace:
    """Get the current component specification as parsed from the docstring."""
    uid = os.urandom(4).hex()
    return _current_spec.get(
        SimpleNamespace(
            name=f"anon_{uid}",
            version=0,
            versioned_name=f"cdf_{uid}_v0",
        )
    )


def connection(type_: str, /, **kwargs: t.Any) -> ConnectionConfig:
    kwargs["type"] = type_
    return _parse_connection_config(kwargs)


def current_workspace() -> Workspace:
    """Get the current workspace."""
    return active_workspace.get()


if t.TYPE_CHECKING:
    import sqlmesh


def current_context(gateway: str | None = None) -> "sqlmesh.Context":
    """Get the current SQLMesh context."""
    return to_context(current_workspace(), gateway).unwrap()


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
    "connection",
    "logger",
    "session",
    "requests",
    "incremental",
    "current_spec",
    "current_workspace",
    "to_context",
]
