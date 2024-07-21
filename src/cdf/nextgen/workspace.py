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
import cdf.nextgen.model as model

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
    service_definitons: t.Iterable[model.ServiceDef] = field(default_factory=tuple)
    """An iterable of service definitions that the workspace provides."""
    source_definitons: t.Iterable[model.SourceDef] = field(default_factory=tuple)
    """An iterable of source definitions that the workspace provides."""
    destination_definitons: t.Iterable[model.DestinationDef] = field(
        default_factory=tuple
    )
    """An iterable of destination definitions that the workspace provides."""
    data_pipelines: t.Iterable[model.DataPipelineDef] = field(default_factory=tuple)
    """An iterable of data pipelines that the workspace provides."""

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
        for service in self.services.values():
            self.container.add_definition(service.name, service.dependency)
        for source in self.sources.values():
            self.container.add_definition(source.name, source.dependency)
        for destination in self.destinations.values():
            self.container.add_definition(destination.name, destination.dependency)

    def _parse_definitions(
        self,
        defs: t.Iterable[model.TComponentDef],
        into: t.Type[model.TComponent],
    ) -> t.Dict[str, model.TComponent]:
        """Parse a list of component definitions into a lookup."""
        objs = {}
        for obj in defs:
            if isinstance(obj, dict):
                obj = into(**obj)
            obj.dependency.apply_decorators(self.apply)
            objs[obj.name] = obj
        return objs

    @cached_property
    def services(self) -> t.Dict[str, model.Service]:
        """Return the services of the workspace."""
        return self._parse_definitions(self.service_definitons, model.Service)

    @cached_property
    def sources(self) -> t.Dict[str, model.Source]:
        """Return the sources of the workspace."""
        return self._parse_definitions(self.source_definitons, model.Source)

    @cached_property
    def destinations(self) -> t.Dict[str, model.Destination]:
        """Return the destinations of the workspace."""
        return self._parse_definitions(self.destination_definitons, model.Destination)

    @cached_property
    def pipelines(self) -> t.Dict[str, model.DataPipeline]:
        """Return the data pipelines of the workspace."""
        return self._parse_definitions(self.data_pipelines, model.DataPipeline)

    # TODO: this is a stub
    def run_pipeline(self, pipeline: str) -> None:
        """Run a data pipeline by name."""
        load_info = self.invoke(self.pipelines[pipeline])
        print(load_info)  # ...

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
        @click.argument("pipeline", type=click.Choice(list(self.pipelines.keys())))
        def run(pipeline: str) -> None:
            """Run a data pipeline."""
            self.run_pipeline(pipeline)

        return run

    def apply(self, func_or_cls: t.Callable[P, T]) -> t.Callable[..., T]:
        """Wrap a function with configuration and dependencies defined in the workspace."""
        return self.container.wire(self.conf_resolver.resolve_defaults(func_or_cls))

    def invoke(self, func_or_cls: t.Callable[P, T], *args: t.Any, **kwargs: t.Any) -> T:
        """Invoke a function with configuration and dependencies defined in the workspace."""
        return self.apply(func_or_cls)(*args, **kwargs)


if __name__ == "__main__":
    import dlt

    def some_pipeline(source_a, temp_duckdb):
        pipeline = dlt.pipeline("some_pipeline", destination=memory_duckdb)
        load_info = pipeline.run(source_a)
        return load_info

    @dlt.source
    def test_source(a: int, prod_bigquery: str):

        @dlt.resource
        def test_resource():
            yield from [{"a": a, "prod_bigquery": prod_bigquery}]

        return [test_resource]

    memory_duckdb = dlt.destinations.duckdb(":memory:")

    # Define a workspace
    datateam = Workspace(
        name="data-team",
        version="0.1.1",
        configuration_sources=[
            {
                "sfdc": {"username": "abc"},
                "bigquery": {"project_id": "project-123"},
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
                    injector.map_config_section("sfdc")(
                        lambda username: f"https://sfdc.com/{username}"
                    )
                ),
                owner="RevOps",
            ),
        ],
        source_definitons=[
            model.Source(
                "source_a",
                injector.Dependency.prototype(test_source),
                owner="Alex",
                description="Source A",
            )
        ],
        destination_definitons=[
            model.Destination(
                "temp_duckdb",
                injector.Dependency.instance(memory_duckdb),
                owner="Alex",
                description="In-memory DuckDB",
            )
        ],
        data_pipelines=[
            model.DataPipeline(
                "some_pipeline",
                injector.Dependency.prototype(some_pipeline),
                owner="Alex",
                description="A test pipeline",
            )
        ],
    )

    datateam.cli()
