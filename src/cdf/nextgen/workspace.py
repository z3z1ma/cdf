import abc
import os
import string
import typing as t
from collections import ChainMap
from dataclasses import dataclass
from enum import Enum
from functools import cached_property

from typing_extensions import ParamSpec

import cdf.injector as injector
import cdf.nextgen.models as model

T = t.TypeVar("T")
P = ParamSpec("P")


class AbstractWorkspace(abc.ABC):
    """An abstract CDF workspace that allows for dependency injection and configuration resolution."""

    name: str = "default"
    version: str = "0.1.0"

    _configuration: injector.ConfigResolver
    _container: injector.DependencyRegistry

    @abc.abstractmethod
    def get_environment(self) -> str:
        """Return the environment name associated with workspace runtime."""
        pass

    @abc.abstractmethod
    def get_config_sources(self) -> t.Iterable[injector.ConfigSource]:
        """Return an iterable of configuration sources."""
        pass

    @cached_property
    def configuration(self) -> injector.ConfigResolver:
        """Return the configuration resolver for the workspace."""
        for source in self.get_config_sources():
            self._configuration.import_(source)
        self._configuration.set_environment(self.get_environment())
        return self._configuration

    @abc.abstractmethod
    def get_services(self) -> t.Iterable[model.ServiceDef]:
        """Produces a list of service definitions that the workspace provides."""
        pass

    @cached_property
    def services(self) -> t.Tuple[model.Service, ...]:
        """Return the services of the workspace."""
        service_definitons = self.get_services()
        services = []
        for service in service_definitons:
            if isinstance(service, dict):
                service = model.Service(**service)
            if callable(service.dependency.factory):
                service.dependency = injector.Dependency(
                    self.configuration.inject_defaults(service.dependency.factory),
                    *service.dependency[1:],
                )
            services.append(service)
        return tuple(services)

    @abc.abstractmethod
    def get_sources(self) -> t.Iterable[model.SourceDef]:
        """Produces a list of source definitions that the workspace provides."""
        pass

    @cached_property
    def sources(self) -> t.Tuple[model.Source, ...]:
        """Return the sources of the workspace."""
        source_definitons = self.get_sources()
        sources = []
        for source in source_definitons:
            if isinstance(source, dict):
                source = model.Source(**source)
            if callable(source.dependency.factory):
                source.dependency = injector.Dependency(
                    self.configuration.inject_defaults(source.dependency.factory),
                    *source.dependency[1:],
                )
            sources.append(source)
        return tuple(sources)

    @cached_property
    def container(self) -> injector.DependencyRegistry:
        """Return the populated DI container for the workspace."""
        self._container.add_definition(
            "cdf_config", injector.Dependency.from_instance(self._configuration)
        )
        for service in self.services:
            self._container.add_definition(service.name, service.dependency)
        for source in self.sources:
            self._container.add_definition(source.name, source.dependency)
        return self._container

    def add_dependency(
        self, name: injector.DependencyKey, definition: injector.Dependency
    ) -> None:
        """Add a dependency to the workspace DI container."""
        self._container.add_definition(name, definition)

    def import_config(self, config: injector.ConfigSource) -> None:
        """Import a new configuration source into the workspace configuration resolver."""
        self._configuration.import_(config)

    @property
    def cli(self) -> t.Callable:
        """Dynamically generate a CLI entrypoint for the workspace."""
        import click

        @click.command()
        def entrypoint():
            click.echo(f"Hello, {self.name} {self.version}!")

        return entrypoint

    def invoke(self, func_or_cls: t.Callable[P, T], *args: t.Any, **kwargs: t.Any) -> T:
        """Invoke a function with configuration and dependencies defined in the workspace."""
        configured = self.configuration.inject_defaults(func_or_cls)
        return self.container.wire(configured)(*args, **kwargs)


class Workspace(AbstractWorkspace):
    """Base workspace implementation that provides a default configuration and DI container.

    This class can be extended to provide a custom workspace implementation with some common heuristics.
    """

    name: str = "default"
    version: str = "0.1.0"

    def __init__(
        self,
        dependency_registry: injector.DependencyRegistry = injector.DependencyRegistry(),
        configuration: injector.ConfigResolver = injector.ConfigResolver(),
    ) -> None:
        """Initialize the workspace."""
        self._container = dependency_registry
        self._configuration = configuration

    def get_environment(self) -> str:
        """Return the environment of the workspace."""
        return os.getenv("CDF_ENVIRONMENT", "dev")

    def get_config_sources(self) -> t.Iterable[injector.ConfigSource]:
        """Return an iterable of configuration sources."""
        return ["cdf.toml", "cdf.yaml", "cdf.json", "~/.cdf.toml"]

    def get_services(self) -> t.Iterable[model.ServiceDef]:
        """Return a iterable of services that the workspace provides."""
        return []

    def get_sources(self) -> t.Iterable[model.SourceDef]:
        """Return an iterable of sources that the workspace provides."""
        return []


if __name__ == "__main__":
    # Example usage

    # Modularity is acheived through regular Python modules

    # from workspaces.datateam.services import SFDC, Asana, BigQuery, Snowflake
    # from workspaces.datateam.config import CONFIG
    # from workspaces.datateam.sources import salesforce_source

    # Define a workspace
    class DataTeamWorkspace(Workspace):
        name = "data-team"
        version = "0.1.1"

        def get_services(self) -> t.Iterable[model.ServiceDef]:
            # These can be used by simply using the name of the service in a function argument
            return [
                model.Service(
                    "a",
                    injector.Dependency(1),
                    owner="Alex",
                    description="A secret number",
                    sla=model.ServiceLevelAgreement.CRITICAL,
                ),
                model.Service(
                    "b", injector.Dependency(lambda a: a + 1 * 5 / 10), owner="Alex"
                ),
                model.Service(
                    "prod_bigquery", injector.Dependency("dwh-123"), owner="DataTeam"
                ),
                model.Service(
                    "sfdc",
                    injector.Dependency(
                        injector.map_section("sfdc")(
                            lambda username: f"https://sfdc.com/{username}"
                        )
                    ),
                    owner="RevOps",
                ),
            ]

        def get_config_sources(self) -> t.Iterable[injector.ConfigSource]:
            return [
                # STATIC_CONFIG,
                {
                    "sfdc": {"username": "abc"},
                    "bigquery": {"project_id": ...},
                },
                *super().get_config_sources(),
            ]

        def get_sources(self) -> t.Iterable[model.SourceDef]:
            import dlt

            @dlt.source
            def test_source(a: int, prod_bigquery: str):

                @dlt.resource
                def test_resource():
                    return [{"a": a, "prod_bigquery": prod_bigquery}]

            return [
                model.Source(
                    "source_a",
                    injector.Dependency(test_source),
                    owner="Alex",
                    description="Source A",
                )
            ]

    # Create an instance of the workspace
    datateam = DataTeamWorkspace()

    @injector.map_values(secret_number="a.b.c")
    def c(secret_number: int, sfdc: str) -> int:
        print(f"SFDC: {sfdc=}")
        return secret_number * 10

    # Imperatively add dependencies or config if needed
    datateam.add_dependency("c", injector.Dependency(c))
    datateam.configuration.import_({"a.b.c": 10})

    def source_a(a: int, prod_bigquery: str):
        print(f"Source A: {a=}, {prod_bigquery=}")

    # Some interface examples
    print(datateam.name)
    print(datateam.invoke(source_a))
    print(datateam.configuration["sfdc.username"])
    print(datateam.container.get_or_raise("sfdc"))
    print(datateam.invoke(c))

    # Run the autogenerated CLI
    datateam.cli()
