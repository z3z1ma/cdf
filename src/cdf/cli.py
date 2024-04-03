"""CLI for cdf."""

import typing as t
from pathlib import Path

import rich
import typer

import cdf.core.context as context
from cdf.core.project import get_project
from cdf.core.runtime import execute_pipeline_specification

app = typer.Typer(
    rich_markup_mode="rich",
    epilog="Made with [red]♥[/red] by [bold]z3z1ma[/bold].",
    add_completion=False,
    no_args_is_help=True,
)

console = rich.console.Console()


@app.callback()
def main(
    ctx: typer.Context,
    workspace: str,
    path: Path = typer.Option(".", "--path", "-p", help="Path to the project."),
):
    """CDF is a data pipeline framework."""
    ctx.obj = workspace, path


@app.command()
def init(ctx: typer.Context):
    """Initialize a new project."""
    typer.echo(ctx.obj)


@app.command(rich_help_panel="Integrate")
def pipeline(
    ctx: typer.Context,
    source_to_dest: t.Annotated[
        str,
        typer.Argument(
            help="The source and destination of the pipeline separated by a colon."
        ),
    ],
    resources: t.List[str] = typer.Option(
        ...,
        "-r",
        "--resource",
        default_factory=lambda: [],
        help="Glob pattern for resources to run. Can be specified multiple times.",
    ),
    excludes: t.List[str] = typer.Option(
        ...,
        "-x",
        "--exclude",
        default_factory=lambda: [],
        help="Glob pattern for resources to exclude. Can be specified multiple times.",
    ),
    replace: t.Annotated[
        bool,
        typer.Option(
            ...,
            "-F",
            "--force-replace",
            help="Force the write disposition to replace ignoring state. Useful to force a reload of incremental resources.",
        ),
    ] = False,
) -> t.Any:
    """:inbox_tray: Ingest data from a [b blue]pipeline[/b blue] into a data store where it can be [b red]Transformed[/b red].

    \f
    Args:
        ctx: The CLI context.
        source_to_dest: The source and destination of the pipeline separated by a colon.
        resources: The resources to ingest as a sequence of glob patterns.
        excludes: The resources to exclude as a sequence of glob patterns.
        replace: Whether to force replace the write disposition.
    """
    workspace_name, path = ctx.obj
    workspace = (
        get_project(path).bind(lambda p: p.get_workspace(workspace_name)).unwrap()
    )
    context.inject_cdf_config_provider(workspace)
    token = context.active_project.set(workspace)
    try:
        source, destination = source_to_dest.split(":", 1)
        spec = workspace.get_pipeline(source).unwrap()
        exports = execute_pipeline_specification(
            spec,
            destination,
            select=resources,
            exclude=excludes,
            force_replace=replace,
        )
        typer.echo(spec.metric_state if spec.metric_state else "No metrics captured")
        return (
            exports.unwrap()
        )  # maybe a function which searches for LoadInfo objects from the exports
    finally:
        context.active_project.reset(token)


@app.command(rich_help_panel="Inspect")
def discover(
    ctx: typer.Context,
    pipeline: t.Annotated[
        str,
        typer.Argument(help="The pipeline in which to discover resources."),
    ],
    no_quiet: t.Annotated[
        bool,
        typer.Option(
            help="Pipeline stdout is suppressed by default, this disables that."
        ),
    ] = False,
) -> None:
    """:mag: Evaluates a :zzz: Lazy [b blue]pipeline[/b blue] and enumerates the discovered resources.

    \f
    Args:
        ctx: The CLI context.
        pipeline: The pipeline in which to discover resources.
        no_quiet: Whether to suppress the pipeline stdout.
    """
    workspace_name, path = ctx.obj
    workspace = (
        get_project(path).bind(lambda p: p.get_workspace(workspace_name)).unwrap()
    )
    context.inject_cdf_config_provider(workspace)
    token = context.active_project.set(workspace)

    try:
        spec = workspace.get_pipeline(pipeline).unwrap()
        for i, source in enumerate(
            execute_pipeline_specification(
                spec, "dummy", intercept_sources=True, quiet=not no_quiet
            ).unwrap()
        ):
            console.print(f"{i}: {source.name}")
            for j, resource in enumerate(source.resources.values(), 1):
                console.print(
                    f"{i}.{j}: {resource.name} (enabled: {resource.selected})"
                )
    finally:
        context.active_project.reset(token)


if __name__ == "__main__":
    app()
