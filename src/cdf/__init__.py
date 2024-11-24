"""Continous Data Framework (CDF) is a framework for building and deploying data pipelines."""

import cdf.core.configuration as config
import cdf.core.deploy as deploy
import cdf.core.extract_load as el
import cdf.core.models as mdl
import cdf.core.state as state
import cdf.core.test as test
import cdf.core.transform as tr
from cdf.core.container import (
    GLOBAL_CONTAINER,
    Container,
    active_container,
    inject_deps,
    injected,
    register_dep,
)
from cdf.core.project import DataPackage, Project

__all__ = [
    "deploy",
    "el",
    "mdl",
    "state",
    "test",
    "tr",
    "config",
    "Container",
    "active_container",
    "inject_deps",
    "injected",
    "register_dep",
    "DataPackage",
    "Project",
    "GLOBAL_CONTAINER",
]
