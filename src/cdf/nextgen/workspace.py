"""A workspace is a container for services, sources, and configuration that can be used to wire up a data pipeline."""

import os
import sys
import time
import typing as t
from dataclasses import dataclass, field
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
    data_publishers: t.Iterable[model.DataPublisherDef] = field(default_factory=tuple)
    """An iterable of data publishers that the workspace provides."""
    operation_definitions: t.Iterable[model.OperationDef] = field(default_factory=tuple)
    """An iterable of generic operations that the workspace provides."""

    def __post_init__(self) -> None:
        """Initialize the workspace."""
        for source in self.configuration_sources:
            self.conf_resolver.import_(source)
        self.conf_resolver.set_environment(self.environment)
        self.container.add_definition(
            "cdf_workspace",
            injector.Dependency.instance(self),
            override=True,
        )
        self.container.add_definition(
            "cdf_environment",
            injector.Dependency.instance(self.environment),
            override=True,
        )
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
        *additional_decorators: t.Callable,
    ) -> t.Dict[str, model.TComponent]:
        """Parse a list of component definitions into a lookup."""
        objs = {}
        for obj in defs:
            if isinstance(obj, dict):
                obj = into.wrap(**obj)
            objs[obj.name] = obj.apply_wrappers(self.apply, *additional_decorators)
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

    @cached_property
    def publishers(self) -> t.Dict[str, model.DataPublisher]:
        """Return the data publishers of the workspace."""
        return self._parse_definitions(self.data_publishers, model.DataPublisher)

    @cached_property
    def operations(self) -> t.Dict[str, model.Operation]:
        """Return the operations of the workspace."""
        return self._parse_definitions(self.operation_definitions, model.Operation)

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

        @click.group()
        def cli() -> None:
            """A dynamically generated CLI for the workspace."""
            pass

        def _list(d: t.Dict[str, model.TComponent]) -> int:
            for name in d.keys():
                click.echo(name)
            return 1

        cli.command("list-services")(lambda: _list(self.services))
        cli.command("list-sources")(lambda: _list(self.sources))
        cli.command("list-destinations")(lambda: _list(self.destinations))
        cli.command("list-pipelines")(lambda: _list(self.pipelines))
        cli.command("list-publishers")(lambda: _list(self.publishers))
        cli.command("list-operations")(lambda: _list(self.operations))

        @cli.command("run-pipeline")
        @click.argument(
            "pipeline",
            required=False,
            type=click.Choice(list(self.pipelines.keys())),
        )
        @click.option(
            "--test",
            is_flag=True,
            help="Run the pipelines integration test if defined.",
        )
        @click.option("-a", "--arg", nargs=2, multiple=True)
        @click.pass_context
        def run_pipeline(
            ctx: click.Context,
            pipeline: t.Optional[str] = None,
            test: bool = False,
            arg: t.List[t.Tuple[str, str]] = [],
        ) -> None:
            """Run a data pipeline."""
            # Prompt for a pipeline if not specified
            if pipeline is None:
                pipeline = click.prompt(
                    "Enter a pipeline",
                    type=click.Choice(list(self.pipelines.keys())),
                    show_choices=True,
                )
                if pipeline is None:
                    ctx.fail("Pipeline must be specified.")

            # Get the pipeline definition
            pipeline_definition = self.pipelines[pipeline]

            # Run the integration test if specified
            if test:
                if not pipeline_definition.integration_test:
                    ctx.fail("Pipeline does not have an integration test.")
                click.echo("Running integration test.")
                if pipeline_definition.integration_test():
                    click.echo("Integration test passed.")
                    ctx.exit(0)
                else:
                    ctx.fail("Integration test failed.")

            # Run the pipeline
            start = time.time()
            click.echo((info := pipeline_definition()) or "No load info returned.")
            click.echo(
                f"Pipeline process finished in {time.time() - start:.2f} seconds."
            )

            # Check for failed jobs
            if info and info.has_failed_jobs:
                ctx.fail("Pipeline failed.")
            ctx.exit(0)

        @cli.command("run-publisher")
        @click.argument(
            "publisher", required=False, type=click.Choice(list(self.publishers.keys()))
        )
        @click.option(
            "--skip-preflight-check",
            is_flag=True,
            help="Skip the pre-check for the publisher.",
        )
        @click.pass_context
        def run_publisher(
            ctx: click.Context,
            publisher: t.Optional[str] = None,
            skip_preflight_check: bool = False,
        ) -> None:
            """Run a data publisher."""
            # Prompt for a publisher if not specified
            if publisher is None:
                publisher = click.prompt(
                    "Enter a publisher",
                    type=click.Choice(list(self.publishers.keys())),
                    show_choices=True,
                )
                if publisher is None:
                    ctx.fail("Publisher must be specified.")

            # Get the publisher definition
            publisher_definition = self.publishers[publisher]

            # Optionally run the preflight check
            if not skip_preflight_check:
                if not publisher_definition.preflight_check():
                    ctx.fail("Preflight-check failed.")

            # Run the publisher
            start = time.time()
            click.echo(publisher_definition())
            click.echo(
                f"Publisher process finished in {time.time() - start:.2f} seconds."
            )
            ctx.exit(0)

        @cli.command("run-operation")
        @click.argument(
            "operation", required=False, type=click.Choice(list(self.operations.keys()))
        )
        @click.pass_context
        def run_operation(ctx: click.Context, operation: t.Optional[str] = None) -> int:
            """Run an operation."""
            # Prompt for an operation if not specified
            if operation is None:
                operation = click.prompt(
                    "Enter an operation",
                    type=click.Choice(list(self.operations.keys())),
                    show_choices=True,
                )
                if operation is None:
                    ctx.fail("Operation must be specified.")

            # Get the operation definition
            operation_definition = self.operations[operation]

            # Run the operation
            ctx.exit(operation_definition())

        return cli

    def apply(self, func_or_cls: t.Callable[P, T]) -> t.Callable[..., T]:
        """Wrap a function with configuration and dependencies defined in the workspace."""
        return self.container.wire(self.conf_resolver.resolve_defaults(func_or_cls))

    def invoke(self, func_or_cls: t.Callable[P, T], *args: t.Any, **kwargs: t.Any) -> T:
        """Invoke a function with configuration and dependencies defined in the workspace."""
        return self.apply(func_or_cls)(*args, **kwargs)


if __name__ == "__main__":
    import dlt
    import duckdb
    from dlt.common.destination import Destination
    from dlt.sources import DltSource

    def test_pipeline(
        source_a: DltSource, destination: Destination, cdf_environment: str
    ):
        pipeline = dlt.pipeline("some_pipeline", destination=destination)
        print("Running pipeline")
        load_info = pipeline.run(source_a)
        print("Pipeline finished")
        with pipeline.sql_client() as client:
            print("Querying DuckDB in " + cdf_environment)
            print(
                client.execute_sql("SELECT * FROM some_pipeline_dataset.test_resource")
            )
        return load_info

    @dlt.source
    def test_source(a: int, prod_bigquery: str):
        @dlt.resource
        def test_resource():
            print("Reading from API")
            yield from [{"a": a, "prod_bigquery": prod_bigquery}]

        return [test_resource]

    memory_duckdb = dlt.destinations.duckdb(duckdb.connect(":memory:"))

    # Switch statement on environment
    # to scaffold a FF provider, which is hereforward dictated by the user
    # instead of implicit?

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
            ),
            model.Destination(
                "dev_sandbox",
                injector.Dependency.instance(memory_duckdb),
                owner="Alex",
                description="In-memory DuckDB",
            ),
        ],
        data_pipelines=[
            model.DataPipeline(
                "exchangerate_pipeline",
                injector.Dependency.prototype(test_pipeline),
                owner="Alex",
                description="A test pipeline",
            )
        ],
    )

    datateam.cli()
