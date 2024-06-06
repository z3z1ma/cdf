"""CLI for cdf."""

import itertools
import json
import os
import subprocess
import sys
import tempfile
import typing as t
from contextvars import Token
from enum import Enum
from pathlib import Path

import dlt
import pydantic
import rich
import typer
from dlt.common.utils import update_dict_nested
from dlt.common.versioned_state import (
    generate_state_version_hash,
    json_decode_state,
    json_encode_state,
)

import cdf.core.constants as c
import cdf.core.context as context
import cdf.core.logger as logger
from cdf.core.project import (
    FeatureFlagSettings,
    FilesystemSettings,
    Workspace,
    load_project,
)
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
    log_level: t.Optional[str] = typer.Option(
        None,
        "--log-level",
        "-l",
        help="The log level to use.",
        envvar="LOG_LEVEL",
    ),
) -> None:
    """CDF (continuous data framework) is a framework for end to end data processing."""
    ctx.obj = workspace, path
    if debug:
        context.debug_mode.set(True)
    if environment:
        os.environ[c.CDF_ENVIRONMENT] = environment
    if log_level:
        os.environ["RUNTIME__LOG_LEVEL"] = log_level.upper()
    logger.configure(log_level.upper() if log_level else "INFO")
    logger.monkeypatch_dlt()
    logger.monkeypatch_sqlglot()


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
        context.active_workspace.reset(token)


@app.command(rich_help_panel="Core")
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
    """:inbox_tray: Ingest data from a [b blue]Pipeline[/b blue] into a data store where it can be [b red]Transformed[/b red].

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
            workspace.get_sink_spec(destination)
            .map(lambda s: s.get_ingest_config())
            .unwrap_or((destination, None))
        )
        return (
            workspace.get_pipeline_spec(source)
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
        context.active_workspace.reset(token)


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
    """:mag: Dry run a [b blue]Pipeline[/b blue] and enumerates the discovered resources.

    \f
    Args:
        ctx: The CLI context.
        pipeline: The pipeline in which to discover resources.
        no_quiet: Whether to suppress the pipeline stdout.
    """
    workspace, token = _unwrap_workspace(*ctx.obj)
    try:
        spec = workspace.get_pipeline_spec(pipeline).unwrap()
        for i, source in enumerate(
            execute_pipeline_specification(
                spec, "dummy", dry_run=True, quiet=not no_quiet
            )
            .map(lambda rv: rv.pipeline.tracked_sources)
            .unwrap()
        ):
            console.print(f"{i}: {source.name}")
            for j, resource in enumerate(source.resources.values(), 1):
                console.print(
                    f"{i}.{j}: {resource.name} (enabled: {resource.selected})"
                )
    finally:
        context.active_workspace.reset(token)


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
        spec = workspace.get_pipeline_spec(pipeline).unwrap()
        target = next(
            filter(
                lambda r: r.name == resource,
                (
                    resource
                    for src in execute_pipeline_specification(
                        spec, "dummy", dry_run=True, quiet=True
                    )
                    .map(lambda rv: rv.pipeline.tracked_sources)
                    .unwrap()
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
        context.active_workspace.reset(token)


@app.command(rich_help_panel="Core")
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
            workspace.get_publisher_spec(publisher)
            .bind(
                lambda p: execute_publisher_specification(
                    p, workspace.get_transform_context(source), skip_verification
                )
            )
            .unwrap()
        )
    finally:
        context.active_workspace.reset(token)


@app.command(rich_help_panel="Core")
def script(
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
            workspace.get_script_spec(script)
            .bind(lambda s: execute_script_specification(s, capture_stdout=quiet))
            .unwrap()
        )
    finally:
        context.active_workspace.reset(token)


@app.command(rich_help_panel="Core")
def notebook(
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
            workspace.get_notebook_spec(notebook)
            .bind(
                lambda s: execute_notebook_specification(
                    s,
                    storage=workspace.project.filesystem.wrapped,
                    **json.loads(params),
                )
            )
            .unwrap()
        )
    finally:
        context.active_workspace.reset(token)


@app.command(
    rich_help_panel="Utilities",
    context_settings={"allow_extra_args": True, "ignore_unknown_options": True},
)
def jupyter_lab(
    ctx: typer.Context,
) -> None:
    """:star2: Start a Jupyter Lab server in the context of a workspace."""
    workspace, token = _unwrap_workspace(*ctx.obj)
    try:
        subprocess.run(
            ["jupyter", "lab", *ctx.args],
            cwd=workspace.path,
            check=False,
            env={
                **os.environ,
                "PYTHONPATH": ":".join(
                    (
                        str(workspace.path.resolve()),
                        *sys.path,
                        str(workspace.path.parent.resolve()),
                    )
                ),
            },
        )
    finally:
        context.active_workspace.reset(token)


class _SpecType(str, Enum):
    """An enum of specs which can be described via the `spec` command."""

    pipeline = "pipeline"
    publisher = "publisher"
    script = "script"
    notebook = "notebook"
    sink = "sink"
    feature_flags = "feature_flags"
    filesystem = "filesystem"


@app.command(rich_help_panel="Develop")
def spec(
    name: _SpecType,
    json_schema: bool = False,
) -> None:
    """:blue_book: Print the fields for a given spec type.

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
        for spec in t.get_args(FeatureFlagSettings):
            _print(spec)
    elif name == _SpecType.filesystem:
        _print(FilesystemSettings)
    else:
        raise ValueError(f"Invalid spec type {name}.")


class _ExportFormat(str, Enum):
    """An enum of export formats which can be used with the `export` command."""

    json = "json"
    yaml = "yaml"
    yml = "yml"
    py = "py"
    python = "python"
    dict = "dict"


app.add_typer(
    schema := typer.Typer(
        rich_markup_mode="rich",
        epilog="Made with [red]♥[/red] by [bold]z3z1ma[/bold].",
        add_completion=False,
        no_args_is_help=True,
    ),
    name="schema",
    help=":construction: Schema management commands.",
    rich_help_panel="Develop",
)


@schema.command("dump")
def schema_dump(
    ctx: typer.Context,
    pipeline_to_sink: t.Annotated[
        str,
        typer.Argument(
            help="The pipeline:sink combination from which to fetch the schema."
        ),
    ],
    format: t.Annotated[
        _ExportFormat, typer.Option(help="The format to dump the schema in.")
    ] = _ExportFormat.json,
) -> None:
    """:computer: Dump the schema of a [b blue]pipeline[/b blue]:[violet]sink[/violet] combination.

    \f
    Args:
        ctx: The CLI context.
        pipeline_to_sink: The pipeline:sink combination from which to fetch the schema.
        format: The format to dump the schema in.

    Raises:
        typer.BadParameter: If the pipeline or sink are not found.
    """
    workspace, token = _unwrap_workspace(*ctx.obj)
    try:
        source, destination = pipeline_to_sink.split(":", 1)
        sink, _ = (
            workspace.get_sink_spec(destination)
            .map(lambda s: s.get_ingest_config())
            .unwrap_or((destination, None))
        )
        spec = workspace.get_pipeline_spec(source).unwrap()
        rv = execute_pipeline_specification(
            spec, sink, dry_run=True, quiet=True
        ).unwrap()
        if format == _ExportFormat.json:
            console.print(rv.pipeline.default_schema.to_pretty_json())
        elif format in (_ExportFormat.py, _ExportFormat.python, _ExportFormat.dict):
            console.print(rv.pipeline.default_schema.to_dict())
        elif format in (_ExportFormat.yaml, _ExportFormat.yml):
            console.print(rv.pipeline.default_schema.to_pretty_yaml())
        else:
            raise ValueError(
                f"Invalid format {format}. Must be one of {list(_ExportFormat)}"
            )
    finally:
        context.active_workspace.reset(token)


@schema.command("edit")
def schema_edit(
    ctx: typer.Context,
    pipeline_to_sink: t.Annotated[
        str,
        typer.Argument(
            help="The pipeline:sink combination from which to fetch the schema."
        ),
    ],
) -> None:
    """:pencil: Edit the schema of a [b blue]pipeline[/b blue]:[violet]sink[/violet] combination using the system editor.

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
            workspace.get_sink_spec(destination)
            .map(lambda s: s.get_ingest_config())
            .unwrap_or((destination, None))
        )
        spec = workspace.get_pipeline_spec(source).unwrap()
        logger.info(f"Clearing local schema and state for {source}.")
        pipe = spec.create_pipeline(dlt.Pipeline, destination=sink, staging=None)
        pipe.drop()
        logger.info(f"Syncing schema for {source}:{destination}.")
        rv = execute_pipeline_specification(
            spec, sink, dry_run=True, quiet=True
        ).unwrap()
        schema = rv.pipeline.default_schema.clone()
        with tempfile.TemporaryDirectory() as tmpdir:
            fname = f"{schema.name}.schema.yaml"
            with open(os.path.join(tmpdir, fname), "w") as f:
                f.write(schema.to_pretty_yaml())
            logger.info(f"Editing schema {schema.name}.")
            subprocess.run([os.environ.get("EDITOR", "vi"), f.name], check=True)
            pipe_mut = spec.create_pipeline(
                dlt.Pipeline, import_schema_path=tmpdir, destination=sink, staging=None
            )
            schema_mut = pipe_mut.default_schema
            if schema_mut.version > schema.version:
                with pipe_mut.destination_client() as client:
                    logger.info(
                        f"Updating schema {schema.name} to version {schema_mut.version} in {destination}."
                    )
                    client.update_stored_schema()
                logger.info("Schema updated.")
            else:
                logger.info("Schema not updated.")
    finally:
        context.active_workspace.reset(token)


app.add_typer(
    state := typer.Typer(
        rich_markup_mode="rich",
        epilog="Made with [red]♥[/red] by [bold]z3z1ma[/bold].",
        add_completion=False,
        no_args_is_help=True,
    ),
    name="state",
    help=":construction: State management commands.",
    rich_help_panel="Develop",
)


@state.command("dump")
def state_dump(
    ctx: typer.Context,
    pipeline_to_sink: t.Annotated[
        str,
        typer.Argument(
            help="The pipeline:sink combination from which to fetch the schema."
        ),
    ],
) -> None:
    """:computer: Dump the state of a [b blue]pipeline[/b blue]:[violet]sink[/violet] combination.

    \f
    Args:
        ctx: The CLI context.
        pipeline_to_sink: The pipeline:sink combination from which to fetch the state.

    Raises:
        typer.BadParameter: If the pipeline or sink are not found.
    """
    workspace, token = _unwrap_workspace(*ctx.obj)
    try:
        source, destination = pipeline_to_sink.split(":", 1)
        sink, _ = (
            workspace.get_sink_spec(destination)
            .map(lambda s: s.get_ingest_config())
            .unwrap_or((destination, None))
        )
        spec = workspace.get_pipeline_spec(source).unwrap()
        rv = execute_pipeline_specification(
            spec, sink, dry_run=True, quiet=True
        ).unwrap()
        console.print(rv.pipeline.state)
    finally:
        context.active_workspace.reset(token)


@state.command("edit")
def state_edit(
    ctx: typer.Context,
    pipeline_to_sink: t.Annotated[
        str,
        typer.Argument(
            help="The pipeline:sink combination from which to fetch the state."
        ),
    ],
) -> None:
    """:pencil: Edit the state of a [b blue]pipeline[/b blue]:[violet]sink[/violet] combination using the system editor.

    \f
    Args:
        ctx: The CLI context.
        pipeline_to_sink: The pipeline:sink combination from which to fetch the state.

    Raises:
        typer.BadParameter: If the pipeline or sink are not found.
    """
    workspace, token = _unwrap_workspace(*ctx.obj)
    try:
        source, destination = pipeline_to_sink.split(":", 1)
        sink, _ = (
            workspace.get_sink_spec(destination)
            .map(lambda s: s.get_ingest_config())
            .unwrap_or((destination, None))
        )
        spec = workspace.get_pipeline_spec(source).unwrap()
        logger.info(f"Clearing local state and state for {source}.")
        pipe = spec.create_pipeline(dlt.Pipeline, destination=sink, staging=None)
        pipe.drop()
        logger.info(f"Syncing state for {source}:{destination}.")
        rv = execute_pipeline_specification(
            spec, sink, dry_run=True, quiet=True
        ).unwrap()
        with (
            tempfile.NamedTemporaryFile(suffix=".json") as tmp,
            rv.pipeline.managed_state(extract_state=True) as state,
        ):
            pre_hash = generate_state_version_hash(state, exclude_attrs=["_local"])
            tmp.write(
                json.dumps(json.loads(json_encode_state(state)), indent=2).encode()
            )
            tmp.flush()
            logger.info(f"Editing state in {destination}.")
            subprocess.run([os.environ.get("EDITOR", "vi"), tmp.name], check=True)
            with open(tmp.name, "r") as f:
                update_dict_nested(t.cast(dict, state), json_decode_state(f.read()))
            post_hash = generate_state_version_hash(state, exclude_attrs=["_local"])
        if pre_hash != post_hash:
            execute_pipeline_specification(
                spec, sink, select=[], exclude=["*"], quiet=True
            ).unwrap()
            logger.info("State updated.")
        else:
            logger.info("State not updated.")
    finally:
        context.active_workspace.reset(token)


app.add_typer(
    model := typer.Typer(
        rich_markup_mode="rich",
        epilog="Made with [red]♥[/red] by [bold]z3z1ma[/bold].",
        add_completion=False,
        no_args_is_help=True,
    ),
    name="model",
    help=":construction: Model management commands.",
    rich_help_panel="Core",
)


@model.command("evaluate")
def model_evaluate(
    ctx: typer.Context,
    model: t.Annotated[
        str,
        typer.Argument(help="The model to evaluate. Can be prefixed with the gateway."),
    ],
    start: str = typer.Option(
        "1 month ago",
        help="The start time to evaluate the model from. Defaults to 1 month ago.",
    ),
    end: str = typer.Option(
        "now",
        help="The end time to evaluate the model to. Defaults to now.",
    ),
    limit: t.Optional[int] = typer.Option(
        None, help="The number of rows to limit the evaluation to."
    ),
) -> None:
    """:bar_chart: Evaluate a [b red]Model[/b red] and print the results. A thin wrapper around `sqlmesh evaluate`

    \f
    Args:
        ctx: The CLI context.
        model: The model to evaluate. Can be prefixed with the gateway.
        limit: The number of rows to limit the evaluation to.
    """
    workspace, token = _unwrap_workspace(*ctx.obj)
    try:
        if ":" in model:
            gateway, model = model.split(":", 1)
        else:
            gateway = None
        sqlmesh_ctx = workspace.get_transform_context(gateway)
        console.print(
            sqlmesh_ctx.evaluate(
                model,
                limit=limit,
                start=start,
                end=end,
                execution_time="now",
            )
        )
    finally:
        context.active_workspace.reset(token)


@model.command("render")
def model_render(
    ctx: typer.Context,
    model: t.Annotated[
        str,
        typer.Argument(help="The model to evaluate. Can be prefixed with the gateway."),
    ],
    start: str = typer.Option(
        "1 month ago",
        help="The start time to evaluate the model from. Defaults to 1 month ago.",
    ),
    end: str = typer.Option(
        "now",
        help="The end time to evaluate the model to. Defaults to now.",
    ),
    expand: t.List[str] = typer.Option([], help="The referenced models to expand."),
    dialect: t.Optional[str] = typer.Option(
        None, help="The SQL dialect to use for rendering."
    ),
) -> None:
    """:bar_chart: Render a [b red]Model[/b red] and print the query. A thin wrapper around `sqlmesh render`

    \f
    Args:
        ctx: The CLI context.
        model: The model to evaluate. Can be prefixed with the gateway.
        start: The start time to evaluate the model from. Defaults to 1 month ago.
        end: The end time to evaluate the model to. Defaults to now.
        expand: The referenced models to expand.
        dialect: The SQL dialect to use for rendering.
    """
    workspace, token = _unwrap_workspace(*ctx.obj)
    try:
        if ":" in model:
            gateway, model = model.split(":", 1)
        else:
            gateway = None
        sqlmesh_ctx = workspace.get_transform_context(gateway)
        console.print(
            sqlmesh_ctx.render(
                model, start=start, end=end, execution_time="now", expand=expand
            ).sql(dialect or sqlmesh_ctx.default_dialect, pretty=True)
        )
    finally:
        context.active_workspace.reset(token)


@model.command("name")
def model_name(
    ctx: typer.Context,
    model: t.Annotated[
        str,
        typer.Argument(
            help="The model to convert the physical name. Can be prefixed with the gateway."
        ),
    ],
) -> None:
    """:bar_chart: Get a [b red]Model[/b red]'s physical table name. A thin wrapper around `sqlmesh table_name`

    \f
    Args:
        ctx: The CLI context.
        model: The model to evaluate. Can be prefixed with the gateway.
    """
    workspace, token = _unwrap_workspace(*ctx.obj)
    try:
        if ":" in model:
            gateway, model = model.split(":", 1)
        else:
            gateway = None
        sqlmesh_ctx = workspace.get_transform_context(gateway)
        console.print(sqlmesh_ctx.table_name(model, False))
    finally:
        context.active_workspace.reset(token)


@model.command("diff")
def model_diff(
    ctx: typer.Context,
    model: t.Annotated[
        str,
        typer.Argument(help="The model to evaluate. Can be prefixed with the gateway."),
    ],
    source_target: t.Annotated[
        str,
        typer.Argument(help="The source and target environments separated by a colon."),
    ],
    show_sample: bool = typer.Option(
        False, help="Whether to show a sample of the diff."
    ),
) -> None:
    """:bar_chart: Compute the diff of a [b red]Model[/b red] across 2 environments. A thin wrapper around `sqlmesh table_diff`

    \f
    Args:
        ctx: The CLI context.
        model: The model to evaluate. Can be prefixed with the gateway.
        source_target: The source and target environments separated by a colon.
    """
    workspace, token = _unwrap_workspace(*ctx.obj)
    try:
        if ":" in model:
            gateway, model = model.split(":", 1)
        else:
            gateway = None
        sqlmesh_ctx = workspace.get_transform_context(gateway)
        source, target = source_target.split(":", 1)
        sqlmesh_ctx.table_diff(
            source, target, model_or_snapshot=model, show_sample=show_sample
        )
    finally:
        context.active_workspace.reset(token)


@model.command("prototype")
def model_prototype(
    ctx: typer.Context,
    dependencies: t.List[str] = typer.Option(
        [],
        "-d",
        "--dependencies",
        help="The dependencies to include in the prototype.",
    ),
    start: str = typer.Option(
        "1 month ago",
        help="The start time to evaluate the model from. Defaults to 1 month ago.",
    ),
    end: str = typer.Option(
        "now",
        help="The end time to evaluate the model to. Defaults to now.",
    ),
    limit: int = typer.Option(
        5_000_000,
        help="The number of rows to limit the evaluation to.",
    ),
):
    workspace, token = _unwrap_workspace(*ctx.obj)
    try:
        sqlmesh_ctx = workspace.get_transform_context()
        for dep in dependencies:
            df = sqlmesh_ctx.evaluate(
                dep,
                start=start,
                end=end,
                execution_time="now",
                limit=limit,
            )
            df.to_parquet(f"{dep}.parquet", index=False)
    finally:
        context.active_workspace.reset(token)


def _unwrap_workspace(workspace_name: str, path: Path) -> t.Tuple["Workspace", "Token"]:
    """Unwrap the workspace from the context."""
    workspace = (
        load_project(path).bind(lambda p: p.get_workspace(workspace_name)).unwrap()
    )
    workspace.inject_configuration().__enter__()
    token = context.active_workspace.set(workspace)
    maybe_log_level = dlt.config.get("runtime.log_level", str)
    if maybe_log_level:
        logger.set_level(maybe_log_level.upper())
    return workspace, token


if __name__ == "__main__":
    app()
