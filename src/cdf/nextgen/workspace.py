"""A workspace is a container for services, sources, and configuration that can be used to wire up a data pipeline."""

import abc
import os
import string
import typing as t
from collections import ChainMap
from dataclasses import dataclass, field
from enum import Enum
from functools import cached_property

from typing_extensions import ParamSpec

import cdf.injector as injector
import cdf.nextgen.models as model

T = t.TypeVar("T")
P = ParamSpec("P")


@dataclass(frozen=True)
class Workspace:
    """A CDF workspace that allows for dependency injection and configuration resolution."""

    name: str = "default"
    """A human-readable name for the workspace."""
    version: str = "0.1.0"
    """A semver version string for the workspace."""
    environment: str = field(
        default_factory=lambda: os.getenv("CDF_ENVIRONMENT", "dev")
    )
    """The runtime environment used to resolve configuration."""
    conf_resolver: injector.ConfigResolver = field(
        default_factory=injector.ConfigResolver
    )
    """The configuration resolver for the workspace."""
    container: injector.DependencyRegistry = field(
        default_factory=injector.DependencyRegistry
    )
    """The dependency injection container for the workspace."""
    configuration_sources: t.Iterable[injector.ConfigSource] = (
        "cdf.toml",
        "cdf.yaml",
        "cdf.json",
        "~/.cdf.toml",
    )
    """A list of configuration sources resolved and merged by the workspace."""
    service_definitons: t.List[model.ServiceDef] = field(default_factory=list)
    """A list of service definitions that the workspace provides."""
    source_definitons: t.List[model.SourceDef] = field(default_factory=list)
    """A list of source definitions that the workspace provides."""

    def __post_init__(self) -> None:
        """Initialize the workspace."""
        for source in self.configuration_sources:
            self.conf_resolver.import_(source)
        self.conf_resolver.set_environment(self.environment)
        self.container.add_definition(
            "cdf_config",
            injector.Dependency.instance(self.conf_resolver),
            override=True,
        )
        for service in self.services:
            self.container.add_definition(service.name, service.dependency)
        for source in self.sources:
            self.container.add_definition(source.name, source.dependency)

    @cached_property
    def services(self) -> t.Tuple[model.Service, ...]:
        """Return the services of the workspace."""
        services = []
        for service in self.service_definitons:
            if isinstance(service, dict):
                service = model.Service(**service)
            service.dependency = service.dependency.apply_decorators(self.apply)
            services.append(service)
        return tuple(services)

    @cached_property
    def sources(self) -> t.Tuple[model.Source, ...]:
        """Return the sources of the workspace."""
        sources = []
        for source in self.source_definitons:
            if isinstance(source, dict):
                source = model.Source(**source)
            source.dependency = source.dependency.apply_decorators(self.apply)
            sources.append(source)
        return tuple(sources)

    def add_dependency(
        self, name: injector.DependencyKey, definition: injector.Dependency
    ) -> None:
        """Add a dependency to the workspace DI container."""
        self.container.add_definition(name, definition)

    def import_config(self, config: injector.ConfigSource) -> None:
        """Import a new configuration source into the workspace configuration resolver."""
        self.conf_resolver.import_(config)

    @property
    def cli(self) -> t.Callable:
        """Dynamically generate a CLI entrypoint for the workspace."""
        import click

        @click.command()
        def entrypoint():
            click.echo(f"Hello, {self.name} {self.version}!")

        return entrypoint

    def apply(self, func_or_cls: t.Callable[P, T]) -> t.Callable[..., T]:
        """Wrap a function with configuration and dependencies defined in the workspace."""
        return self.container.wire(self.conf_resolver.resolve_defaults(func_or_cls))

    def invoke(self, func_or_cls: t.Callable[P, T], *args: t.Any, **kwargs: t.Any) -> T:
        """Invoke a function with configuration and dependencies defined in the workspace."""
        return self.apply(func_or_cls)(*args, **kwargs)
