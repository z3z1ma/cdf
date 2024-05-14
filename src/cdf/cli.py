"""CLI for cdf."""

import itertools
import json
import os
import subprocess
import sys
import typing as t
from contextvars import Token
from enum import Enum
from pathlib import Path

import pydantic
import rich
import typer

import cdf.core.constants as c
import cdf.core.context as context
from cdf.core.feature_flag import FlagProvider
from cdf.core.project import Workspace, load_project
from cdf.core.runtime import (
    execute_notebook_specification,
    execute_pipeline_specification,
    execute_publisher_specification,
    execute_script_specification,
)
from cdf.core.specification import (
    NotebookSpecification,
    PipelineSpecification,
    PublisherSpecification,
    ScriptSpecification,
    SinkSpecification,
)

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
    path: Path = typer.Option(
        ".", "--path", "-p", help="Path to the project.", envvar="CDF_ROOT"
    ),
    debug: bool = typer.Option(False, "--debug", "-d", help="Enable debug mode."),
    environment: t.Optional[str] = typer.Option(
        None, "--env", "-e", help="Environment to use."
    ),
) -> None:
    """CDF (continuous data framework) is a framework for end to end data processing."""
    ctx.obj = workspace, path
    if debug:
        context.debug_mode.set(True)
    if environment:
        os.environ[c.CDF_ENVIRONMENT] = environment


@app.command(rich_help_panel="Project Management")
def init(ctx: typer.Context) -> None:
    """:art: Initialize a new project."""
    typer.echo(ctx.obj)


@app.command(rich_help_panel="Project Management")
def index(ctx: typer.Context) -> None:
    """:page_with_curl: Print an index of [b][blue]Pipelines[/blue], [red]Models[/red], [yellow]Publishers[/yellow][/b], and other components."""
    workspace, token = _unwrap_workspace(*ctx.obj)
    try:
        console.print("Pipelines", workspace.pipelines)
        console.print("Sinks", workspace.sinks)
        console.print("Publishers", workspace.publishers)
        console.print("Scripts", workspace.scripts)
        console.print("Notebooks", workspace.notebooks)
    finally:
        context.active_project.reset(token)


@app.command(rich_help_panel="Data Management")
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
        sink, stage = (
            workspace.get_sink(destination)
            .map(lambda s: s.get_ingest_config())
            .unwrap_or((destination, None))
        )
        return (
            workspace.get_pipeline(source)
            .bind(
                lambda p: execute_pipeline_specification(
                    p,
                    sink,
                    stage,
                    select=select,
                    exclude=exclude,
                    force_replace=force_replace,
                    enable_stage=not no_stage,
                )
            )
            .unwrap()
        )  # maybe a function which searches for LoadInfo objects from the exports
    finally:
        context.active_project.reset(token)


@app.command(rich_help_panel="Develop")
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
                spec, "dummy", dry_run=True, quiet=not no_quiet
            ).unwrap()
        ):
            console.print(f"{i}: {source.name}")
            for j, resource in enumerate(source.resources.values(), 1):
                console.print(
                    f"{i}.{j}: {resource.name} (enabled: {resource.selected})"
                )
    finally:
        context.active_project.reset(token)


@app.command(rich_help_panel="Develop")
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
                        spec, "dummy", dry_run=True, quiet=True
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


@app.command(rich_help_panel="Data Management")
def publish(
    ctx: typer.Context,
    sink_to_publisher: t.Annotated[
        str,
        typer.Argument(help="The sink and publisher separated by a colon."),
    ],
    skip_verification: t.Annotated[
        bool,
        typer.Option(
            help="Skip the verification of the publisher dependencies.",
        ),
    ] = False,
) -> t.Any:
    """:outbox_tray: [b yellow]Publish[/b yellow] data from a data store to an [violet]External[/violet] system.

    \f
    Args:
        ctx: The CLI context.
        sink_to_publisher: The sink and publisher separated by a colon.
        skip_verification: Whether to skip the verification of the publisher dependencies.
    """
    workspace, token = _unwrap_workspace(*ctx.obj)
    try:
        source, publisher = sink_to_publisher.split(":", 1)
        return (
            workspace.get_publisher(publisher)
            .bind(
                lambda p: execute_publisher_specification(
                    p, workspace.get_transform_context(source), skip_verification
                )
            )
            .unwrap()
        )
    finally:
        context.active_project.reset(token)


@app.command("execute-script", rich_help_panel="Utilities")
def execute_script(
    ctx: typer.Context,
    script: t.Annotated[str, typer.Argument(help="The script to execute.")],
    quiet: t.Annotated[bool, typer.Option(help="Suppress the script stdout.")] = False,
) -> t.Any:
    """:hammer: Execute a [b yellow]Script[/b yellow] within the context of the current workspace.

    \f
    Args:
        ctx: The CLI context.
        script: The script to execute.
        quiet: Whether to suppress the script stdout.
    """
    workspace, token = _unwrap_workspace(*ctx.obj)
    try:
        return (
            workspace.get_script(script)
            .bind(lambda s: execute_script_specification(s, capture_stdout=quiet))
            .unwrap()
        )
    finally:
        context.active_project.reset(token)


@app.command("execute-notebook", rich_help_panel="Utilities")
def execute_notebook(
    ctx: typer.Context,
    notebook: t.Annotated[str, typer.Argument(help="The notebook to execute.")],
    params: t.Annotated[
        str,
        typer.Option(
            ...,
            help="The parameters to pass to the notebook as a json formatted string.",
        ),
    ] = "{}",
) -> t.Any:
    """:notebook: Execute a [b yellow]Notebook[/b yellow] within the context of the current workspace.

    \f
    Args:
        ctx: The CLI context.
        notebook: The notebook to execute.
        params: The parameters to pass to the notebook as a json formatted string.
    """
    workspace, token = _unwrap_workspace(*ctx.obj)
    try:
        return (
            workspace.get_notebook(notebook)
            .bind(
                lambda s: execute_notebook_specification(
                    s, storage=workspace.filesystem, **json.loads(params)
                )
            )
            .unwrap()
        )
    finally:
        context.active_project.reset(token)


@app.command(
    "jupyter-lab",
    rich_help_panel="Utilities",
    context_settings={"allow_extra_args": True, "ignore_unknown_options": True},
)
def jupyter_lab(
    ctx: typer.Context,
) -> None:
    """:notebook: Start a Jupyter Lab server."""
    workspace, token = _unwrap_workspace(*ctx.obj)
    try:
        subprocess.run(
            ["jupyter", "lab", *ctx.args],
            cwd=workspace.root,
            check=False,
            env={
                **os.environ,
                "PYTHONPATH": ":".join(
                    (
                        str(workspace.root.resolve()),
                        *sys.path,
                        str(workspace.root.parent.resolve()),
                    )
                ),
            },
        )
    finally:
        context.active_project.reset(token)


class _SpecType(str, Enum):
    """An enum of specs which can be described via the `spec` command."""

    pipeline = "pipeline"
    publisher = "publisher"
    script = "script"
    notebook = "notebook"
    sink = "sink"
    feature_flags = "feature_flags"


@app.command(rich_help_panel="Develop")
def spec(
    name: _SpecType,
    json_schema: bool = False,
) -> None:
    """:mag: Print the fields for a given spec type.

    \f
    Args:
        name: The name of the spec to print.
        json_schema: Whether to print the JSON schema for the spec.
    """

    def _print_spec(spec: t.Type[pydantic.BaseModel]) -> None:
        console.print(f"[bold]{spec.__name__}:[/bold]")
        for name, info in spec.model_fields.items():
            typ = getattr(info.annotation, "__name__", info.annotation)
            desc = info.description or "No description provided."
            d = f"- [blue]{name}[/blue] ({typ!s}): {desc}"
            if "Undefined" not in str(info.default):
                d += f" Defaults to `{info.default}`)"
            console.print(d)
        console.print()

    def _print(s: t.Type[pydantic.BaseModel]) -> None:
        console.print(s.model_json_schema()) if json_schema else _print_spec(s)

    if name == _SpecType.pipeline:
        _print(PipelineSpecification)
    elif name == _SpecType.publisher:
        _print(PublisherSpecification)
    elif name == _SpecType.script:
        _print(ScriptSpecification)
    elif name == _SpecType.notebook:
        _print(NotebookSpecification)
    elif name == _SpecType.sink:
        _print(SinkSpecification)
    elif name == _SpecType.feature_flags:
        for spec in t.get_args(FlagProvider):
            _print(spec)
    else:
        raise ValueError(f"Invalid spec type {name}.")


@app.command(rich_help_panel="Develop")
def export_schema(
    ctx: typer.Context,
    pipeline_to_sink: t.Annotated[
        str,
        typer.Argument(
            help="The pipeline:sink combination from which to fetch the schema."
        ),
    ],
) -> None:
    """:wrench: Prints the first N rows of a [b green]Resource[/b green] within a [b blue]pipeline[/b blue]. Defaults to [cyan]5[/cyan].

    This is useful for quickly inspecting data :detective: and verifying that it is coming over the wire correctly.

    \f
    Args:
        ctx: The CLI context.
        pipeline_to_sink: The pipeline:sink combination from which to fetch the schema.

    Raises:
        typer.BadParameter: If the pipeline or sink are not found.
    """
    workspace, token = _unwrap_workspace(*ctx.obj)
    try:
        source, destination = pipeline_to_sink.split(":", 1)
        sink, _ = (
            workspace.get_sink(destination)
            .map(lambda s: s.get_ingest_config())
            .unwrap_or((destination, None))
        )
        spec = workspace.get_pipeline(source).unwrap()

        for src in execute_pipeline_specification(
            spec, sink, dry_run=True, quiet=True
        ).unwrap():
            ...
    finally:
        context.active_project.reset(token)


def _unwrap_workspace(workspace_name: str, path: Path) -> t.Tuple["Workspace", "Token"]:
    """Unwrap the workspace from the context."""
    workspace = (
        load_project(path).bind(lambda p: p.get_workspace(workspace_name)).unwrap()
    )
    context.inject_cdf_config_provider(workspace)
    token = context.active_project.set(workspace)
    return workspace, token


if __name__ == "__main__":
    app()
