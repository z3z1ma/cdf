"""Continous Data Framework (CDF) is a framework for building and deploying data pipelines."""

# Expose the core modules
import cdf.legacy.adapter as adapter
import cdf.legacy.configuration as config
import cdf.legacy.deploy as deploy
import cdf.legacy.interface as interface

# Directly export these symbols
from cdf.legacy.container import (
    GLOBAL_CONTAINER,
    Container,
    active_container,
    inject_deps,
    injected,
    register_dep,
)
from cdf.legacy.project import DataPackage, Project


def get_container() -> Container:
    """Shorthand sugar for active_container.get"""
    return active_container.get(GLOBAL_CONTAINER)


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
    "get_container",
]
