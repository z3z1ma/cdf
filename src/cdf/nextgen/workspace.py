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
    configuration: injector.ConfigResolver = field(
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
            self.configuration.import_(source)
        self.configuration.set_environment(self.environment)
        self.container.add_definition(
            "cdf_config",
            injector.Dependency.from_instance(self.configuration),
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
        self.configuration.import_(config)

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
        return self.container.wire(self.configuration.resolve_defaults(func_or_cls))

    def invoke(self, func_or_cls: t.Callable[P, T], *args: t.Any, **kwargs: t.Any) -> T:
        """Invoke a function with configuration and dependencies defined in the workspace."""
        return self.apply(func_or_cls)(*args, **kwargs)


if __name__ == "__main__":
    # Example usage

    # Modularity is acheived through regular Python modules

    # from workspaces.datateam.services import SFDC, Asana, BigQuery, Snowflake
    # from workspaces.datateam.config import CONFIG
    # from workspaces.datateam.sources import salesforce_source

    import dlt

    @dlt.source
    def test_source(a: int, prod_bigquery: str):

        @dlt.resource
        def test_resource():
            yield from [{"a": a, "prod_bigquery": prod_bigquery}]

        return [test_resource]

    # Define a workspace
    datateam = Workspace(
        name="data-team",
        version="0.1.1",
        configuration_sources=[
            # DATATEAM_CONFIG,
            {
                "sfdc": {"username": "abc"},
                "bigquery": {"project_id": ...},
            },
            *Workspace.configuration_sources,
        ],
        service_definitons=[
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
        ],
        source_definitons=[
            model.Source(
                "source_a",
                injector.Dependency(test_source),
                owner="Alex",
                description="Source A",
            )
        ],
    )

    @injector.map_values(secret_number="a.b.c")
    def c(secret_number: int, sfdc: str) -> int:
        print(f"SFDC: {sfdc=}")
        return secret_number * 10

    # Imperatively add dependencies or config if needed
    datateam.add_dependency("c", injector.Dependency(c))
    datateam.import_config({"a.b.c": 10})

    def source_a(a: int, prod_bigquery: str):
        print(f"Source A: {a=}, {prod_bigquery=}")

    # Some interface examples
    print(datateam.name)
    print(datateam.configuration["sfdc.username"])
    print(datateam.container.get_or_raise("sfdc"))
    print(datateam.invoke(source_a))
    print(datateam.invoke(c))
    source = next(iter(datateam.sources))()
    print(list(source))

    # Run the autogenerated CLI
    datateam.cli()
