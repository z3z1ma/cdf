"""CLI for cdf."""

import itertools
import typing as t
from contextvars import Token
from pathlib import Path

import rich
import typer

import cdf.core.context as context
from cdf.core.project import Workspace, get_project
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
    debug: bool = typer.Option(False, "--debug", "-d", help="Enable debug mode."),
):
    """CDF (continuous data framework) is a framework for end to end data processing."""
    ctx.obj = workspace, path
    if debug:
        context.debug_mode.set(True)


@app.command()
def init(ctx: typer.Context):
    """Initialize a new project."""
    typer.echo(ctx.obj)


@app.command()
def index(ctx: typer.Context):
    """Print the project index."""
    workspace, token = _unwrap_workspace(*ctx.obj)
    try:
        console.print(workspace.pipelines)
        console.print(workspace.sinks)
    finally:
        context.active_project.reset(token)


@app.command(rich_help_panel="Integrate")
def pipeline(
    ctx: typer.Context,
    pipeline_to_sink: t.Annotated[
        str,
        typer.Argument(help="The pipeline and sink separated by a colon."),
    ],
    select: t.List[str] = typer.Option(
        ...,
        "-s",
        "--select",
        default_factory=lambda: [],
        help="Glob pattern for resources to run. Can be specified multiple times.",
    ),
    exclude: t.List[str] = typer.Option(
        ...,
        "-x",
        "--exclude",
        default_factory=lambda: [],
        help="Glob pattern for resources to exclude. Can be specified multiple times.",
    ),
    force_replace: t.Annotated[
        bool,
        typer.Option(
            ...,
            "-F",
            "--force-replace",
            help="Force the write disposition to replace ignoring state. Useful to force a reload of incremental resources.",
        ),
    ] = False,
    no_stage: t.Annotated[
        bool,
        typer.Option(
            ...,
            "--no-stage",
            help="Do not stage the data in the staging destination of the sink even if defined.",
        ),
    ] = False,
) -> t.Any:
    """:inbox_tray: Ingest data from a [b blue]pipeline[/b blue] into a data store where it can be [b red]Transformed[/b red].

    \f
    Args:
        ctx: The CLI context.
        pipeline_to_sink: The pipeline and sink separated by a colon.
        select: The resources to ingest as a sequence of glob patterns.
        exclude: The resources to exclude as a sequence of glob patterns.
        force_replace: Whether to force replace the write disposition.
        no_stage: Whether to disable staging the data in the sink.
    """
    workspace, token = _unwrap_workspace(*ctx.obj)
    try:
        source, destination = pipeline_to_sink.split(":", 1)
        pipe = workspace.get_pipeline(source).unwrap()
        sink, stage = (
            workspace.get_sink(destination)
            .map(lambda s: s.sink_ingest())
            .unwrap_or((destination, None))
        )
        exports = execute_pipeline_specification(
            pipe,
            sink,
            stage,
            select=select,
            exclude=exclude,
            force_replace=force_replace,
            enable_stage=not no_stage,
        )
        typer.echo(
            pipe.runtime_metrics if pipe.runtime_metrics else "No metrics captured"
        )
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
    workspace, token = _unwrap_workspace(*ctx.obj)
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


@app.command(rich_help_panel="Inspect")
def head(
    ctx: typer.Context,
    pipeline: t.Annotated[str, typer.Argument(help="The pipeline to inspect.")],
    resource: t.Annotated[str, typer.Argument(help="The resource to inspect.")],
    n: t.Annotated[int, typer.Option("-n", "--rows")] = 5,
) -> None:
    """:wrench: Prints the first N rows of a [b green]Resource[/b green] within a [b blue]pipeline[/b blue]. Defaults to [cyan]5[/cyan].

    This is useful for quickly inspecting data :detective: and verifying that it is coming over the wire correctly.

    \f
    Args:
        ctx: The CLI context.
        pipeline: The pipeline to inspect.
        resource: The resource to inspect.
        n: The number of rows to print.

    Raises:
        typer.BadParameter: If the resource is not found in the pipeline.
    """
    workspace, token = _unwrap_workspace(*ctx.obj)
    try:
        spec = workspace.get_pipeline(pipeline).unwrap()
        target = next(
            filter(
                lambda r: r.name == resource,
                (
                    resource
                    for src in execute_pipeline_specification(
                        spec, "dummy", intercept_sources=True, quiet=True
                    ).unwrap()
                    for resource in src.resources.values()
                ),
            ),
            None,
        )
        if target is None:
            raise typer.BadParameter(
                f"Resource {resource} not found in pipeline {pipeline}.",
                param_hint="resource",
            )
        list(
            map(
                lambda it: console.print(it[1]),
                itertools.takewhile(lambda it: it[0] < n, enumerate(target)),
            )
        )
    finally:
        context.active_project.reset(token)


def _unwrap_workspace(workspace_name: str, path: Path) -> t.Tuple["Workspace", "Token"]:
    """Unwrap the workspace from the context."""
    workspace = (
        get_project(path).bind(lambda p: p.get_workspace(workspace_name)).unwrap()
    )
    context.inject_cdf_config_provider(workspace)
    token = context.active_project.set(workspace)
    return workspace, token


if __name__ == "__main__":
    app()
