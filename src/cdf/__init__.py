"""Continous Data Framework (CDF) is a framework for building and deploying data pipelines."""

# Expose the core modules
import cdf.core.adapter as adapter
import cdf.core.configuration as config
import cdf.core.deploy as deploy
import cdf.core.interface as interface

# Directly export these symbols
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
    "adapter",
    "deploy",
    "interface",
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
