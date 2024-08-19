"""A workspace is a container for components and configurations."""

import os
import time
import typing as t
from functools import cached_property, partialmethod
from pathlib import Path

import pydantic
from typing_extensions import ParamSpec, Self

import cdf.core.component as cmp
import cdf.core.configuration as conf
import cdf.core.context as ctx
import cdf.core.injector as injector

if t.TYPE_CHECKING:
    import click
    import sqlmesh


T = t.TypeVar("T")
P = ParamSpec("P")

__all__ = ["Workspace"]


class Workspace(pydantic.BaseModel, frozen=True):
    """A CDF workspace that allows for dependency injection and configuration resolution."""

    name: str = "default"
    """A human-readable name for the workspace."""
    version: str = "0.1.0"
    """A semver version string for the workspace."""
    environment: str = pydantic.Field(
        default_factory=lambda: os.getenv("CDF_ENVIRONMENT", "dev")
    )
    """The runtime environment used to resolve configuration."""
    conf_resolver: conf.ConfigResolver = pydantic.Field(
        default_factory=conf.ConfigResolver
    )
    """The configuration resolver for the workspace."""
    container: injector.DependencyRegistry = pydantic.Field(
        default_factory=injector.DependencyRegistry
    )
    """The dependency injection container for the workspace."""
    configuration_sources: t.Iterable[conf.ConfigSource] = (
        "cdf.toml",
        "cdf.yaml",
        "cdf.json",
        "~/.cdf.toml",
    )
    """A list of configuration sources resolved and merged by the workspace."""
    service_definitions: t.Iterable[cmp.ServiceDef] = ()
    """An iterable of raw service definitions that the workspace provides."""
    pipeline_definitions: t.Iterable[cmp.DataPipelineDef] = ()
    """An iterable of raw pipeline definitions that the workspace provides."""
    publishers_definitions: t.Iterable[cmp.DataPublisherDef] = ()
    """An iterable of raw publisher definitions that the workspace provides."""
    operation_definitions: t.Iterable[cmp.OperationDef] = ()
    """An iterable of raw generic operation definitions that the workspace provides."""

    # TODO: define an adapter for transformation providers
    sqlmesh_path: t.Optional[t.Union[str, Path]] = None
    """The path to the sqlmesh root for the workspace."""
    sqlmesh_context_kwargs: t.Dict[str, t.Any] = {}
    """Keyword arguments to pass to the sqlmesh context."""

    if t.TYPE_CHECKING:
        # PERF: this is a workaround for pydantic not being able to build a model with a forwardref
        # we still want the deferred import at runtime
        sqlmesh_context_class: t.Optional[t.Type[sqlmesh.Context]] = None
        """A custom context class to use for sqlmesh."""
    else:
        sqlmesh_context_class: t.Optional[t.Type[t.Any]] = None
        """A custom context class to use for sqlmesh."""

    @pydantic.model_validator(mode="after")
    def _setup(self) -> Self:
        """Initialize the workspace."""
        for source in self.configuration_sources:
            self.conf_resolver.import_source(source)
        self.conf_resolver.set_environment(self.environment)
        self.container.add_from_dependency(
            "cdf_workspace",
            injector.Dependency.instance(self),
            override=True,
        )
        self.container.add_from_dependency(
            "cdf_environment",
            injector.Dependency.instance(self.environment),
            override=True,
        )
        self.container.add_from_dependency(
            "cdf_config",
            injector.Dependency.instance(self.conf_resolver),
            override=True,
        )
        self.container.add_from_dependency(
            "cdf_transform",
            injector.Dependency.singleton(self.get_sqlmesh_context_or_raise),
            override=True,
        )
        for service in self.services.values():
            self.container.add_from_dependency(service.name, service.main)
        self.activate()
        return self

    def activate(self) -> Self:
        """Activate the workspace for the current context."""
        ctx.set_active_workspace(self)
        return self

    def _parse_definitions(
        self, defs: t.Iterable[cmp.TComponentDef], into: t.Type[cmp.TComponent]
    ) -> t.Dict[str, cmp.TComponent]:
        """Parse a list of component definitions into a lookup."""
        components = {}
        with ctx.use_workspace(self):
            for definition in defs:
                component = into.model_validate(definition, context={"parent": self})
                components[component.name] = component
        return components

    @cached_property
    def services(self) -> t.Dict[str, cmp.Service]:
        """Return the resolved services of the workspace."""
        return self._parse_definitions(self.service_definitions, cmp.Service)

    @cached_property
    def pipelines(self) -> t.Dict[str, cmp.DataPipeline]:
        """Return the resolved data pipelines of the workspace."""
        return self._parse_definitions(self.pipeline_definitions, cmp.DataPipeline)

    @cached_property
    def publishers(self) -> t.Dict[str, cmp.DataPublisher]:
        """Return the resolved data publishers of the workspace."""
        return self._parse_definitions(self.publishers_definitions, cmp.DataPublisher)

    @cached_property
    def operations(self) -> t.Dict[str, cmp.Operation]:
        """Return the resolved operations of the workspace."""
        return self._parse_definitions(self.operation_definitions, cmp.Operation)

    @t.overload
    def get_sqlmesh_context(
        self,
        gateway: t.Optional[str] = ...,
        must_exist: t.Literal[False] = False,
        **kwargs: t.Any,
    ) -> t.Optional["sqlmesh.Context"]: ...

    @t.overload
    def get_sqlmesh_context(
        self,
        gateway: t.Optional[str] = ...,
        must_exist: t.Literal[True] = True,
        **kwargs: t.Any,
    ) -> "sqlmesh.Context": ...

    def get_sqlmesh_context(
        self, gateway: t.Optional[str] = None, must_exist: bool = False, **kwargs: t.Any
    ) -> t.Optional["sqlmesh.Context"]:
        """Return the transform context or raise an error if not defined."""
        import sqlmesh

        if self.sqlmesh_path is None:
            if must_exist:
                raise ValueError("Transformation provider not defined.")
            return None

        kwargs = {**self.sqlmesh_context_kwargs, **kwargs}
        with ctx.use_workspace(self):
            klass = self.sqlmesh_context_class or sqlmesh.Context
            return klass(paths=[self.sqlmesh_path], gateway=gateway, **kwargs)

    if t.TYPE_CHECKING:

        def get_sqlmesh_context_or_raise(
            self, gateway: t.Optional[str] = None, **kwargs: t.Any
        ) -> "sqlmesh.Context": ...

    else:
        get_sqlmesh_context_or_raise = partialmethod(
            get_sqlmesh_context, must_exist=True
        )

    @property
    def cli(self) -> "click.Group":
        """Dynamically generate a CLI entrypoint for the workspace."""
        import click

        @click.group()
        def cli() -> None:
            """A dynamically generated CLI for the workspace."""
            self.activate()

        def _list(d: t.Dict[str, cmp.TComponent]) -> int:
            for name in sorted(d.keys()):
                click.echo(name)
            return 1

        cli.command("list-services")(lambda: _list(self.services))
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
        @click.pass_context
        def run_pipeline(
            ctx: click.Context, pipeline: t.Optional[str] = None, test: bool = False
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
                    raise click.BadParameter(
                        "Pipeline must be specified.", ctx=ctx, param_hint="pipeline"
                    )

            # Get the pipeline definition
            pipeline_definition = self.pipelines[pipeline]

            # Run the integration test if specified
            if test:
                if not pipeline_definition.integration_test:
                    raise click.UsageError(
                        f"Pipeline `{pipeline}` does not define an integration test."
                    )
                click.echo("Running integration test.", err=True)
                if pipeline_definition.integration_test():
                    click.echo("Integration test passed.", err=True)
                    ctx.exit(0)
                else:
                    ctx.fail("Integration test failed.")

            # Run the pipeline
            start = time.time()
            jobs = pipeline_definition()
            click.echo(
                f"Pipeline process finished in {time.time() - start:.2f} seconds.",
                err=True,
            )

            # Check for failed jobs
            for job in jobs:
                if job.has_failed_jobs:
                    ctx.fail("Pipeline failed.")

            ctx.exit(0)

        @cli.command("run-publisher")
        @click.argument(
            "publisher", required=False, type=click.Choice(list(self.publishers.keys()))
        )
        @click.option(
            "--test",
            is_flag=True,
            help="Run the publishers integration test if defined.",
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
            test: bool = False,
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
                    raise click.BadParameter(
                        "Publisher must be specified.", ctx=ctx, param_hint="publisher"
                    )

            # Get the publisher definition
            publisher_definition = self.publishers[publisher]

            # Run the integration test if specified
            if test:
                if not publisher_definition.integration_test:
                    raise click.UsageError(
                        f"Publisher `{publisher}` does not define an integration test."
                    )
                click.echo("Running integration test.", err=True)
                if publisher_definition.integration_test():
                    click.echo("Integration test passed.", err=True)
                    ctx.exit(0)
                else:
                    ctx.fail("Integration test failed.")

            # Optionally run the preflight check
            if not skip_preflight_check:
                if not publisher_definition.preflight_check():
                    ctx.fail("Preflight-check failed.")

            # Run the publisher
            start = time.time()
            click.echo(publisher_definition())
            click.echo(
                f"Publisher process finished in {time.time() - start:.2f} seconds.",
                err=True,
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
                    raise click.BadParameter(
                        "Operation must be specified.", ctx=ctx, param_hint="operation"
                    )

            # Get the operation definition
            operation_definition = self.operations[operation]

            # Run the operation
            ctx.exit(operation_definition())

        return cli

    def bind(self, func_or_cls: t.Callable[P, T]) -> t.Callable[..., T]:
        """Wrap a function with configuration and dependencies defined in the workspace."""
        configured_f = self.conf_resolver.resolve_defaults(func_or_cls)
        return self.container.wire(configured_f)

    def invoke(self, func_or_cls: t.Callable[P, T], *args: t.Any, **kwargs: t.Any) -> T:
        """Invoke a function with configuration and dependencies defined in the workspace."""
        with ctx.use_workspace(self):
            return self.bind(func_or_cls)(*args, **kwargs)


if __name__ == "__main__":
    import dlt
    import duckdb

    import cdf.core.context as ctx

    @dlt.source
    @ctx.resolve
    def source_a(a: int, prod_bigquery: str):
        @dlt.resource
        def test_resource():
            print("Reading from API")
            yield from [{"a": a, "prod_bigquery": prod_bigquery}]

        return [test_resource]

    memory_duckdb = dlt.destinations.duckdb(duckdb.connect(":memory:"))

    def test_pipeline(
        cdf_environment: str,
    ):
        pipeline = dlt.pipeline("some_pipeline", destination=memory_duckdb)

        def run():
            print("Running pipeline")
            load = pipeline.run(source_a())
            print("Pipeline finished")
            with pipeline.sql_client() as client:
                print("Querying DuckDB in " + cdf_environment)
                print(
                    client.execute_sql(
                        "SELECT * FROM some_pipeline_dataset.test_resource"
                    )
                )
            return load

        return pipeline, run

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
        ],
        service_definitions=[
            cmp.Service(
                name="a",
                main=injector.Dependency(factory=lambda: 1),
                owner="Alex",
                description="A secret number",
                sla=cmp.ServiceLevelAgreement.CRITICAL,
            ),
            cmp.Service(
                name="b",
                main=injector.Dependency(factory=lambda a: a + 1 * 5 / 10),
                owner="Alex",
            ),
            # Example of a service defined with a dict
            {
                "name": "prod_bigquery",
                "main": {
                    "factory": lambda b, project_id: f"dwh-1{b+1:.0f}3?{project_id=}",
                    "lifecycle": "prototype",
                    "conf_spec": ("bigquery",),
                },
                "owner": "DataTeam",
            },
            cmp.Service(
                name="sfdc",
                main=injector.Dependency(
                    factory=lambda username: f"https://sfdc.com/{username}",
                    conf_spec=("sfdc",),
                ),
                owner="RevOps",
            ),
        ],
        pipeline_definitions=[
            cmp.DataPipeline(
                main=test_pipeline,
                integration_test=lambda: True,
                name="exchangerate_pipeline",
                owner="Alex",
                description="A test pipeline",
            )
        ],
    )

    datateam.cli()
