"""CLI for cdf."""
import datetime
import fnmatch
import getpass
import importlib.util
import json
import os
import subprocess
import sys
import tempfile
import typing as t
from concurrent.futures import ThreadPoolExecutor
from enum import Enum
from pathlib import Path

import dlt
import rich
import typer
from croniter import croniter

import cdf.core.constants as c
import cdf.core.context as cdf_context
import cdf.core.logger as logger
from cdf.core.spec.staging import StagingSpecification

if t.TYPE_CHECKING:
    from cdf import Project, SupportsComponentMetadata

T = t.TypeVar("T")

app = typer.Typer(
    rich_markup_mode="rich",
    epilog="Made with [red]♥[/red] by [bold]z3z1ma[/bold].",
    add_completion=False,
    no_args_is_help=True,
)
transform_app = typer.Typer(
    rich_markup_mode="rich",
    epilog="Made with [red]♥[/red] by [bold]z3z1ma[/bold].",
    add_completion=False,
    no_args_is_help=True,
)
app.add_typer(transform_app, name="transform", rich_help_panel="Integrate")


class Delimiter(str, Enum):
    """Enum of delimiters for the CLI."""

    DOT = "."
    DCOLON = "::"
    ARROW = "->"
    DARRROW = ">>"
    PIPE = "|"
    FSLASH = "/"
    TO = "-to-"


@app.callback()
def main(
    ctx: typer.Context,
    root: Path = typer.Option(
        ...,
        "-p",
        "--path",
        default_factory=Path.cwd,
        help="Path to the project root. Defaults to cwd. Parent dirs are searched for a workspace file.",
        envvar="CDF_ROOT",
    ),
    log_level: str = typer.Option(
        "INFO",
        "-l",
        "--log-level",
        help="Set the log level. Defaults to INFO.",
        envvar="CDF_LOG_LEVEL",
    ),
    debug: t.Annotated[
        bool,
        typer.Option(
            ...,
            "-d",
            "--debug",
            help="Run in debug mode, force log level to debug in cdf, dlt, and sqlmesh.",
            envvar="CDF_DEBUG",
        ),
    ] = False,
    install: t.Annotated[
        bool,
        typer.Option(
            ...,
            "-i",
            "--install",
            help="Install the component being invoked during runtime. This is experimental and may be removed in the future.",
            envvar="CDF_AUTO_INSTALL",
        ),
    ] = False,
) -> None:
    """:sparkles: a [b]framework[b] for managing and running [u]continousdataframework[/u] projects. :sparkles:

    [b/]
    - ( :electric_plug: ) [b blue]pipelines[/b blue]    are responsible for fetching data from a data pipeline.
    - ( :shuffle_tracks_button: ) [b red]models[/b red] are responsible for transforming data in a data warehouse.
    - ( :mailbox: ) [b yellow]publishers[/b yellow] are responsible for publishing data to an external system.
    """
    from cdf.core.workspace import Project

    if install:
        cdf_context.enable_autoinstall()

    logger.set_level(log_level.upper() if not debug else "DEBUG")
    logger.monkeypatch_sqlglot()
    logger.monkeypatch_dlt()
    if debug:
        import sqlmesh

        dlt.config["runtime.log_level"] = "DEBUG"
        sqlmesh.configure_logging(force_debug=True)

    if ctx.invoked_subcommand in ("init-project", "init-workspace"):
        return

    ctx.obj = Project.find_nearest(root)


@app.command(rich_help_panel="Project Info")
def index(ctx: typer.Context) -> None:
    """:page_with_curl: Print an index of [b][blue]Pipelines[/blue], [red]Models[/red], and [yellow]Publishers[/yellow][/b] loaded from the pipeline directory paths."""

    project: Project = ctx.obj
    rich.print(f" {project.name}")
    for _, workspace in project.items():
        with workspace.runtime_context():
            rich.print(f"\n ~ {workspace.name}")

            rich.print(f"\n   [blue]Pipelines[/blue]: {len(workspace.pipelines)}")
            for i, (name, meta) in enumerate(workspace.pipelines.items(), start=1):
                rich.print(f"   {i}) {name} ({meta.entrypoint_info})")

            rich.print(f"\n   [red]Models[/red]: {len(workspace.models)}")
            for i, (name, meta) in enumerate(workspace.models.items(), start=1):
                rich.print(f"   {i}) {name} ({meta.kind})")

            rich.print(f"\n   [yellow]Publishers[/yellow]: {len(workspace.publishers)}")
            for i, (name, meta) in enumerate(workspace.publishers.items(), start=1):
                rich.print(f"   {i}) {name} ({meta.entrypoint_info})")

            rich.print(f"\n   [green]Scripts[/green]: {len(workspace.scripts)}")
            for i, (name, meta) in enumerate(workspace.scripts.items(), start=1):
                rich.print(f"   {i}) {name} ({meta.entrypoint_info})")

            rich.print(f"\n   [cyan]Notebooks[/cyan]: {len(workspace.notebooks)}")
            for i, (name, meta) in enumerate(workspace.notebooks.items(), start=1):
                rich.print(f"   {i}) {name} ({meta.entrypoint_info})")

            rich.print(f"\n   [magenta]Sinks[/magenta]: {len(workspace.sinks)}")
            for i, (name, meta) in enumerate(workspace.sinks.items(), start=1):
                rich.print(f"   {i}) {name} ({meta.entrypoint_info})")
    rich.print("")


@app.command(rich_help_panel="Project Info")
def docs(ctx: typer.Context) -> None:
    """:book: Render documentation for the project."""
    project: Project = ctx.obj
    docs_path = project.root.joinpath("docs")
    if not docs_path.exists():
        docs_path.mkdir()
    md_doc = "# CDF Project\n\n"
    for workspace in project.values():
        md_doc += f"## ✨ {workspace.name.title()} Space\n\n"
        if workspace.pipelines:
            md_doc += "### 🚚 Pipelines\n\n"
            for name, meta in workspace.pipelines.items():
                md_doc += _metadata_to_md_section(name, meta)
        if workspace.models:
            md_doc += "### 🔄 Models\n\n"
            for name, meta in workspace.models.items():
                md_doc += _metadata_to_md_section(name, meta)
        if workspace.publishers:
            md_doc += "### 🖋️ Publishers\n\n"
            for name, meta in workspace.publishers.items():
                md_doc += _metadata_to_md_section(name, meta)
        if workspace.scripts:
            md_doc += "### 📜 Scripts\n\n"
            for name, meta in workspace.scripts.items():
                md_doc += _metadata_to_md_section(name, meta)
        if workspace.notebooks:
            md_doc += "### 📓 Notebooks\n\n"
            for name, meta in workspace.notebooks.items():
                md_doc += _metadata_to_md_section(name, meta)
        md_doc += "\n"
    rich.print(md_doc)


@app.command(rich_help_panel="Project Info")
def path(
    ctx: typer.Context,
    workspace: str = typer.Argument(default=None),
) -> None:
    """:file_folder: Print the project root path. Pass a workspace to print the workspace root path.

    This is useful for scripting automation tasks.
    """
    project: Project = ctx.obj
    if workspace:
        print(project[workspace].root.absolute(), file=sys.stdout, flush=True)
    else:
        print(project.root.absolute(), file=sys.stdout, flush=True)


@app.command(rich_help_panel="Inspect")
def discover(
    ctx: typer.Context,
    pipeline: t.Annotated[
        str, typer.Argument(help="The <workspace>.<pipeline> to discover.")
    ],
    opts: str = typer.Argument(
        "{}", help="JSON formatted options to forward to the pipeline."
    ),
) -> None:
    """:mag: Evaluates a :zzz: Lazy [b blue]pipeline[/b blue] and enumerates the discovered resources.

    \f
    Args:
        ctx: The CLI context.
        pipeline: The pipeline to discover.
        opts: JSON formatted options to forward to the pipeline.
    """
    logger.debug("Discovering pipeline %s", pipeline)
    project: Project = ctx.obj
    try:
        ws, src = _parse_ws_component(pipeline, project=project)
    except ValueError as e:
        form = "<workspace>.<pipeline>" if len(project) > 1 else "<pipeline>"
        raise typer.BadParameter(
            f"Must specify a pipeline in the form {form}, got {pipeline!r}; {e}",
            param=ctx.command.params[0],
        ) from e
    workspace = project[ws]
    with workspace.runtime_source(src, **json.loads(opts)) as rt_source:
        rich.print(
            f"\nDiscovered {len(rt_source.resources)} resources in"
            f" [b red]{pipeline}.v{workspace.pipelines[src].version}[/b red]:"
        )
        for i, resource in enumerate(rt_source.resources.values(), start=1):
            if resource.selected:
                rich.print(f"  {i}) [b green]{resource.name}[/b green] (enabled: True)")
            else:
                rich.print(f"  {i}) [b red]{resource.name}[/b red] (enabled: False)")
        _print_metadata(workspace.pipelines[src])


@app.command(rich_help_panel="Inspect")
def head(
    ctx: typer.Context,
    pipeline: t.Annotated[
        str, typer.Argument(help="The <workspace>.<pipeline>.<resource> to inspect.")
    ],
    opts: str = typer.Argument(
        "{}", help="JSON formatted options to forward to the pipeline."
    ),
    num: t.Annotated[int, typer.Option("-n", "--num-rows")] = 5,
) -> None:
    """:wrench: Prints the first N rows of a [b green]Resource[/b green] within a [b blue]pipeline[/b blue]. Defaults to [cyan]5[/cyan].

    This is useful for quickly inspecting data :detective: and verifying that it is coming over the wire correctly.

    \f
    Args:
        ctx: The CLI context.
        pipeline: The pipeline to inspect.
        resource: The resource to inspect.
        opts: JSON formatted options to forward to the pipeline.
        num: The number of rows to print.

    Raises:
        typer.BadParameter: If the resource is not found in the pipeline.
    """
    from cdf.core.utils import flatten_stream

    project: Project = ctx.obj
    try:
        ws, src, resource = _parse_ws_component(pipeline, project=project)
    except ValueError as e:
        form = (
            "<workspace>.<pipeline>.<resource>"
            if len(project) > 1
            else "<pipeline>.<resource>"
        )
        raise typer.BadParameter(
            f"Must specify a pipeline and resource in the form {form}, got {pipeline!r}; {e}",
            param=ctx.command.params[0],
        ) from e
    workspace = project[ws]
    token = cdf_context.LIMIT.set(num)
    with workspace.runtime_source(src, **json.loads(opts)) as rt_source:
        if resource not in rt_source.resources:
            raise typer.BadParameter(
                f"Resource {resource} not found in source for pipeline {pipeline}."
            )
        res = rt_source.resources[resource]
        rich.print(
            f"\nHead of [b red]{resource}[/b red] in [b blue]{pipeline}.v{workspace.pipelines[src].version}[/b blue]:"
        )
        for rec in flatten_stream(res):
            rich.print(rec)
            num -= 1
            if num == 0:
                break
    cdf_context.LIMIT.reset(token)


@app.command(rich_help_panel="Integrate")
def pipeline(
    ctx: typer.Context,
    pipeline: t.Annotated[
        str, typer.Argument(help="The <workspace>.<pipeline>.<sink> to run.")
    ],
    opts: str = typer.Argument(
        "{}", help="JSON formatted options to forward to the pipeline."
    ),
    resources: t.List[str] = typer.Option(
        ..., "-r", "--resource", default_factory=list
    ),
) -> None:
    """:inbox_tray: Ingest data from a [b blue]pipeline[/b blue] into a data store where it can be [b red]Transformed[/b red].

    \f
    Args:
        ctx: The CLI context.
        pipeline: The pipeline to ingest from and the sink to ingest into.
        opts: JSON formatted options to forward to the pipeline.
        resources: The resources to ingest.

    Raises:
        typer.BadParameter: If no resources are selected.
    """
    project: Project = ctx.obj
    try:
        ws, pipe, sink = _parse_ws_component(pipeline, project=project)
    except ValueError as e:
        form = (
            "<workspace>.<pipeline>.<sink>" if len(project) > 1 else "<pipeline>.<sink>"
        )
        raise typer.BadParameter(
            f"Must specify a pipeline and sink in the form {form}, got {pipeline!r}; {e}",
            param=ctx.command.params[0],
        ) from e
    workspace = project[ws]
    with workspace.runtime_context():
        logger.info(
            workspace.pipelines[pipe](workspace, sink, resources, **json.loads(opts))
        )


@transform_app.callback(invoke_without_command=True)
def transform(
    ctx: typer.Context,
    sink: t.Annotated[
        str,
        typer.Argument(
            help="The <workspace>.<sink> to operate in. Workspace can be omitted in a single workspace project."
        ),
    ],
) -> None:
    """:arrows_counterclockwise: [b red]Transform[/b red] data in a database. Entrypoint for [b]SQLMesh[/b] with cdf semantics.

    \f
    This swaps the CLI context to a transform context which makes it compatible with the sqlmesh CLI
    while still allowing us to augment behavior with opinionated defaults.
    """
    project: Project = ctx.obj
    if ctx.invoked_subcommand is None:
        if sink in SQLMESH_COMMANDS and len(project) > 1:
            raise typer.BadParameter(
                f"When running a {sink} command, you must specify a workspace."
                f" For example: cdf transform {next(iter(project.keys()))} {sink}"
            )
        elif sink in project or sink in next(iter(project.values())).sinks:
            ctx.invoke(transform_app, ["--help"])
        else:
            raise typer.BadParameter(
                f"Workspace `{sink}` not found. Available workspaces: {', '.join(project.keys())}"
            )
    try:
        workspace, sink = _parse_ws_component(sink, project=project)
    except ValueError as e:
        form = "<workspace>.<sink>" if len(project) > 1 else "<sink>"
        raise typer.BadParameter(
            f"Must specify a sink in the form {form}, got {sink!r}; {e}",
        ) from e
    workspaces = workspace.split(",")
    main_workspace = workspaces[0]
    # Ensure we have a primary workspace
    if main_workspace == "*":
        raise typer.BadParameter(
            "Cannot run models without a primary workspace. Specify a workspace in the first position."
        )
    # A special case for running a plan with all workspaces accessible to the context
    if any(ws == "*" for ws in workspaces):
        others = set(project.keys()).difference(main_workspace)
        workspaces = [main_workspace, *others]
    # Ensure all workspaces exist and are valid
    for ws in workspaces:
        if ws not in project:
            raise typer.BadParameter(f"Workspace `{ws}` not found.")
    # Swap context to SQLMesh context
    project[main_workspace].runtime_context().__enter__()
    ctx.obj = project.transform_context(*workspaces, sink=sink, load=False)


SQLMESH_COMMANDS = (
    "render",
    "evaluate",
    "format",
    "diff",
    "plan",
    "run",
    "invalidate",
    "dag",
    "test",
    "audit",
    "fetchdf",
    "info",
    "ui",
    "migrate",
    "rollback",
    "create_test",
    "table_diff",
    "rewrite",
)
"""A list of sqlmesh commands worth wrapping."""


def _get_transform_command_wrapper(name: str) -> t.Callable[[typer.Context], None]:
    """Passthrough for sqlmesh commands.

    Args:
        name: The name of the command.

    Returns:
        A function that invokes the sqlmesh command.
    """
    if name not in SQLMESH_COMMANDS:
        raise typer.BadParameter(
            f"Command {name} not found. Must be one of {SQLMESH_COMMANDS}."
        )

    import click
    import sqlmesh.cli.main as sqlmesh

    cmd: click.Command = getattr(sqlmesh, name)
    doc = cmd.help or cmd.callback.__doc__ or f"Run the {name} command"

    def _passthrough(ctx: typer.Context) -> None:
        nonlocal cmd

        if name not in ("create_external_models", "migrate", "rollback", "ui"):
            ctx.obj.load()
        parser = cmd.make_parser(ctx)
        opts, args, _ = parser.parse_args(ctx.args)
        return ctx.invoke(cmd, *args, **opts)

    _passthrough.__name__ = name
    _passthrough.__doc__ = f"{doc} See the CLI reference for options: https://sqlmesh.readthedocs.io/en/stable/reference/cli/#{name}"
    return _passthrough


for passthrough in SQLMESH_COMMANDS:
    transform_app.command(
        passthrough,
        context_settings={"allow_extra_args": True, "ignore_unknown_options": True},
    )(_get_transform_command_wrapper(passthrough))


@app.command(rich_help_panel="Integrate")
def publish(
    ctx: typer.Context,
    publisher: t.Annotated[
        str, typer.Argument(help="the <workspace>.<sink>.<publisher> to run")
    ],
    opts: str = typer.Argument(
        "{}", help="JSON formatted options to forward to the publisher."
    ),
    prompt_on_untracked: bool = typer.Option(
        True,
        help="Prompt the user before publishing untracked data. Defaults to True.",
    ),
) -> None:
    """:outbox_tray: [b yellow]Publish[/b yellow] data from a data store to an [violet]External[/violet] system."""
    from sqlmesh.core.dialect import normalize_model_name

    project: Project = ctx.obj
    try:
        ws, sink, pub_name = _parse_ws_component(publisher, project=project)
    except ValueError as e:
        form = (
            "<workspace>.<sink>.<publisher>"
            if len(project) > 1
            else "<sink>.<publisher>"
        )
        raise typer.BadParameter(
            f"Must specify a publisher in the form {form}, got {publisher!r}; {e}",
            param=ctx.command.params[0],
        ) from e
    workspace = project[ws]
    with workspace.runtime_context():
        pub = workspace.publishers[pub_name]
        context = workspace.transform_context(sink)
        normalized_name = normalize_model_name(
            pub.from_,
            dialect=context.config.dialect,
            default_catalog=context.default_catalog,
        )
        if normalized_name not in context.models:
            logger.warning(
                "Model %s not found in transform context. We cannot track lineage or enforce data quality.",
                pub.from_,
            )
            if prompt_on_untracked:
                typer.confirm(
                    "Model not found in transform context. We cannot track lineage or enforce data quality. Continue?",
                    abort=True,
                )
        else:
            model = context.models[normalized_name]
            # Ensure the model is not missing intervals before publishing
            snapshot = context.get_snapshot(normalized_name)
            assert snapshot, f"Snapshot not found for {normalized_name}"
            if snapshot.missing_intervals(
                datetime.date.today() - datetime.timedelta(days=7),
                datetime.date.today(),
            ):
                logger.error("Model %s has missing intervals. Cannot publish.", model)
                raise typer.Exit()
        pub(context, **json.loads(opts))  # returns rows affected


@app.command("execute-script", rich_help_panel="Utility")
def execute_script(
    ctx: typer.Context,
    script: t.Annotated[str, typer.Argument(help="The <workspace>.<script> to run")],
    opts: str = typer.Argument(
        "{}", help="JSON formatted options to forward to the script."
    ),
) -> None:
    """:rocket: Run a script in a workspace environment.

    A script is an arbitrary python file located in the ./scripts directory of a workspace. It defines an `entrypoint`
    function which takes a reference to the workspace as the first argument. Users can leverage cdf.with_config to
    inject configuration from the cdf_config file. Arbitrary keyword arguments can also be passed to the entrypoint
    function via the opts argument which is JSON formatted.

    \f
    Args:
        ctx: The CLI context.
        script: The script to run.
        opts: JSON formatted options to forward to the script.
    """
    project: Project = ctx.obj
    try:
        ws, script = _parse_ws_component(script, project=project)
    except ValueError as e:
        form = "<workspace>.<script>" if len(project) > 1 else "<script>"
        raise typer.BadParameter(
            f"Must specify a script in the form {form}, got {script!r}; {e}",
            param=ctx.command.params[0],
        ) from e
    workspace = project[ws]
    with workspace.runtime_context():
        workspace.scripts[script](workspace, **json.loads(opts))


@app.command(rich_help_panel="Utility")
def jupyter(
    ctx: typer.Context,
    workspace: str = typer.Argument(
        default=None, help="The <workspace> to open jupyter lab in."
    ),
) -> None:
    """:rocket: Open juptyer lab in a workspace environment.

    \f
    Args:
        ctx: The CLI context.
    """
    project: Project = ctx.obj
    ws = workspace or next(iter(project.keys()))
    with project[ws].runtime_context():
        pythonpath = sys.path.copy()
        env = os.environ.copy()
        env["PYTHONPATH"] = ":".join(pythonpath)
        subprocess.check_call(
            ["jupyter", "lab"],
            cwd=project[ws].root,
            env=env,
        )


@app.command("execute-notebook", rich_help_panel="Utility")
def execute_notebook(
    ctx: typer.Context,
    notebook: t.Annotated[
        str, typer.Argument(help="The <workspace>.<notebook> to run")
    ],
    opts: str = typer.Argument(
        "{}", help="JSON formatted parameters to forward to the notebook."
    ),
) -> None:
    """:rocket: Run a notebook in a workspace environment.

    A notebook is an arbitrary ipynb file located in the ./notebooks directory of a workspace.

    \f
    Args:
        ctx: The CLI context.
        notebook: The notebook to run.
        opts: JSON formatted parameters to forward to the notebook.
    """
    project: Project = ctx.obj
    try:
        ws, notebook = _parse_ws_component(notebook, project=project)
    except ValueError as e:
        form = "<workspace>.<notebook>" if len(project) > 1 else "<notebook>"
        raise typer.BadParameter(
            f"Must specify a notebook in the form {form}, got {notebook!r}; {e}",
            param=ctx.command.params[0],
        ) from e
    workspace = project[ws]
    with workspace.runtime_context():
        origvar = os.environ.get("PYTHONPATH")
        pythonpath = sys.path.copy()
        os.environ["PYTHONPATH"] = ":".join(pythonpath)
        workspace.notebooks[notebook](workspace, **json.loads(opts))
        if origvar is None:
            os.environ.pop("PYTHONPATH")
        else:
            os.environ["PYTHONPATH"] = origvar


@app.command("fetch-metadata", rich_help_panel="Utility")
def fetch_metadata(
    ctx: typer.Context,
    sink: str,
    pipelines: t.List[str] = typer.Option(
        [],
        "-p",
        "--pipeline",
        help="Glob pattern for pipelines to fetch metadata for. Defaults to all. Passing any value will disable fetching metadata for external references. Can be specified multiple times.",
    ),
    unmanaged: bool = typer.Option(
        True,
        "-u",
        "--unmanaged/--skip-unmanaged",
        help="Fetch external references in transformations which are not managed by cdf pipelines.",
    ),
) -> None:
    """:floppy_disk: Regenerate workspace metadata.

    Data is stored in <workspace>/metadata/<destination>/<catalog>.yaml
    This is typically followed by cdf transform generate-staging-layer to
    automatically generate staging layers for each catalog. You can
    then run cdf transform plan to materialize the staging layers.

    \f
    Args:
        ctx: The CLI context.
        workspace: The workspace to regenerate metadata for.
    """
    from ruamel.yaml import YAML
    from sqlmesh.core.dialect import normalize_model_name

    logger.info("Fetching and aggregating metadata from sqlmesh and dlt")
    project: Project = ctx.obj
    yaml = YAML(typ="rt")

    if pipelines and unmanaged:
        logger.info("Skipping unmanaged models since we are fetching a managed subset")
        unmanaged = False
    pipelines = pipelines or ["*"]

    try:
        workspace, sink_name = _parse_ws_component(sink, project=project)
    except ValueError as e:
        form = "<workspace>.<sink>" if len(project) > 1 else "<sink>"
        raise typer.BadParameter(
            f"Must specifc a sink in the form {form}, got {sink!r}; {e}",
            param=ctx.command.params[0],
        ) from e

    ws = project[workspace]
    meta_path = ws.root / c.METADATA / sink_name
    meta_path.mkdir(exist_ok=True)

    with ws.runtime_context():
        seen = set()
        output = {}

        for name, src in ws.pipelines.items():
            if not any(fnmatch.fnmatch(name, pat) for pat in pipelines):
                continue
            with tempfile.TemporaryDirectory() as tmp:
                dataset = f"{name}_v{src.version}"
                pipe = dlt.pipeline(
                    name,
                    dataset_name=dataset,
                    destination=ws.sinks[sink_name]("destination"),
                    pipelines_dir=tmp,
                )
                pipe.sync_destination()
                for schema in pipe.schemas.values():
                    d = output.setdefault(dataset, {})
                    for metadata in schema.data_tables() + schema.dlt_tables():
                        assert metadata["name"]  # for type checking
                        d[(k := f"{dataset}.{metadata['name']}")] = metadata
                        seen.add(k)

        if unmanaged:
            context = ws.transform_context(sink_name, load=False)
            seen = [
                normalize_model_name(
                    name,
                    dialect=context.config.dialect,
                    default_catalog=context.default_catalog,
                )
                for name in seen
            ]

            capture_dump = ws.root / "schema.yaml"
            capture_dump.unlink(missing_ok=True)

            # NOTE: eventually we should implement this ourselves to reduce overhead
            context.create_external_models()

            external_models = yaml.load(capture_dump.read_text()) or []
            capture_dump.unlink(missing_ok=True)

            def _unseen(d: dict) -> bool:
                return (
                    normalize_model_name(
                        d["name"],
                        dialect=context.config.dialect,
                        default_catalog=context.default_catalog,
                    )
                    not in seen
                )

            unmanaged_f = meta_path / c.SQLMESH_METADATA_FILE
            unmanaged_f.parent.mkdir(parents=True, exist_ok=True)
            yaml.dump(list(filter(_unseen, external_models)), unmanaged_f)

    for schema_name, metadata in output.items():
        yaml.dump(metadata, meta_path.joinpath(f"{schema_name}.yaml"))


@app.command("generate-staging-layer", rich_help_panel="Utility")
def generate_staging_layer(
    ctx: typer.Context,
    sink: str,
    fetch_metadata_: bool = typer.Option(
        True,
        "-f",
        "--fetch-metadata/--no-fetch-metadata",
        help="Regenerate metadata before running",
    ),
    tables: t.List[str] = typer.Option(
        [],
        "-t",
        "--table",
        help="Glob pattern for tables to generate staging models for. Defaults to all. Can be specified multiple times.",
    ),
    overwrite: bool = typer.Option(
        False,
        "-o",
        "--overwrite",
        help="Overwrite existing staging models. Defaults to False.",
    ),
    sqlfmt_preset: bool = typer.Option(
        False,
        "-s",
        "--sqlfmt",
        help="A preset which will wrap the MODEL def with a no fmt directive and will format the model body with sqlfmt.",
    ),
) -> None:
    """:floppy_disk: Generate a staging layer for a catalog.

    After fetching metadata, this will generate a staging layer for each catalog. This is typically
    followed by cdf transform plan to materialize the staging layers.

    \f
    Args:
        ctx: The CLI context.
        sink: The sink to generate staging layers for.
        fetch_metadata: Whether to fetch metadata before generating staging layers.
    """
    from sqlglot import exp
    from sqlmesh.core.dialect import format_model_expressions
    from sqlmesh.core.model import create_sql_model

    if sqlfmt_preset:
        maybe_spec = importlib.util.find_spec("sqlfmt")
        if not maybe_spec:
            raise typer.BadParameter(
                "The sqlfmt preset requires sqlfmt to be installed."
            )

    if fetch_metadata_:
        fetch_metadata(ctx, sink, [], True)

    tables = tables or ["*"]

    logger.info("Generating cdf DSL staging layer")
    project: Project = ctx.obj

    try:
        workspace, sink_name = _parse_ws_component(sink, project=project)
    except ValueError as e:
        form = "<workspace>.<sink>" if len(project) > 1 else "<sink>"
        raise typer.BadParameter(
            f"Must specifc a sink in the form {form}, got {sink!r}; {e}",
            param=ctx.command.params[0],
        ) from e

    ws = project[workspace]
    context = ws.transform_context(sink_name)

    def _generate_staging_model(
        ref: exp.Table,
        transformers: t.List[StagingSpecification],
        mapping: t.Dict[str, exp.DataType],
        target: Path,
    ) -> int:
        logger.info("Generating model for %s", target)
        select = exp.select(
            *[exp.cast(exp.column(c, "this"), typ) for c, typ in mapping.items()]
        ).from_(ref.as_("this"))
        mut_ref = ref
        for transform_func in transformers:
            select, mut_ref = transform_func(select)
        if mut_ref == ref:
            msg = "Applicable staging spec did not transform %s reference"
            logger.warning(msg, ref)
            return 0
        renderable = create_sql_model(
            mut_ref.sql(),
            query=select,
            module_path=ws.root,
            dialect=context.config.dialect,
            project=context.config.project,
            time_column_format=context.config.time_column_format,
            physical_schema_override=context.config.physical_schema_override,
            tags=["staging", "cdf", "generated"],
            owner=os.getenv("CDF_USER", getpass.getuser()),
        )
        def_ = renderable.render_definition()
        contents = format_model_expressions(def_, dialect=renderable.dialect)
        if sqlfmt_preset:
            import sqlfmt.api
            import sqlfmt.mode

            bloc, query = contents.split(";", 1)
            fmt_query = sqlfmt.api.format_string(
                query,
                sqlfmt.mode.Mode(line_length=120, single_process=True, quiet=True),
            )
            contents = f"-- fmt: off\n{bloc.strip()};\n-- fmt: on\n\n{fmt_query}"

        logger.info("Writing staging model for %s to %s", ref, target)
        return target.write_text(contents)

    tpe = ThreadPoolExecutor(
        max_workers=(os.cpu_count() or 1) * 2, thread_name_prefix="cdf"
    )
    jobs = []
    for model in context.models.values():
        ref = exp.to_table(model.fqn)
        if model.kind.name.value != "EXTERNAL":
            continue
        if ref.name.startswith("_"):
            continue
        basic_fqn = ".".join(
            map(lambda t: t.strip('"'), (ref.catalog, ref.db, ref.name))
        )
        if not any(fnmatch.fnmatch(basic_fqn, pat) for pat in tables):
            continue
        specs = list(filter(lambda s: s.is_applicable(ref), ws.staging_specs))
        if not specs:
            continue
        target = ws.root / c.MODELS / c.STAGING / ref.db / f"{ref.name}.sql"
        target.parent.mkdir(parents=True, exist_ok=True)
        if target.exists() and not overwrite:
            logger.info("Skipping %s since it already exists", target)
            continue
        jobs.append(
            tpe.submit(
                _generate_staging_model,
                ref,
                specs,
                model.columns_to_types_or_raise,
                target,
            )
        )
    if not all(job.result() for job in jobs):
        raise typer.Exit(1)
    tpe.shutdown()


@app.command("init-workspace", rich_help_panel="Project Initialization")
def init_workspace(
    directory: t.Annotated[
        Path,
        typer.Argument(
            help="The directory to initialize the workspace in. Must be empty.",
            dir_okay=True,
            file_okay=False,
            resolve_path=True,
        ),
    ] = Path.cwd(),
) -> None:
    """:art: Initialize a new workspace.

    \f
    Args:
        directory: The directory to initialize the workspace in. Must be empty.
    """
    directory.mkdir(parents=True, exist_ok=True)
    if any(os.listdir(directory)):
        raise typer.BadParameter("Directory must be empty.")
    logger.info("Initializing workspace in %s", directory)
    for dir_ in c.DIR_LAYOUT:
        # Basic directory layout
        directory.joinpath(*dir_.split(".")).mkdir(parents=True, exist_ok=False)
    directory.joinpath(c.CONFIG_FILE).touch()
    directory.joinpath(".env").touch()
    directory.joinpath(".gitignore").write_text(
        "\n".join(
            [
                "__pycache__",
                "*.pyc",
                ".env",
                ".cache",
                "logs",
                "*.duckdb",
                "*.duckdb.wal",
            ]
        )
    )


@app.command("init-project", rich_help_panel="Project Initialization")
def init_project(
    ctx: typer.Context,
    directories: t.Annotated[
        t.List[Path],
        typer.Argument(
            help="The directory to initialize the project in. Must be empty.",
            dir_okay=True,
            file_okay=False,
            resolve_path=False,
        ),
    ],
    root: t.Annotated[
        Path,
        typer.Option(
            ..., "-r", "--root", help="The directory to initialize the project."
        ),
    ] = Path.cwd(),
) -> None:
    """:art: Initialize a new project in the current directory.

    \f
    Args:
        root: The directory to initialize the project in.
        directories: The directories in which to inialize workspaces relative to the project root.
    """
    import tomlkit

    root.mkdir(parents=True, exist_ok=True)
    if any(d.is_absolute() for d in directories if d.exists()):
        raise typer.BadParameter("Directories must be relative paths.")
    if any(os.listdir(root / d) for d in directories if d.exists()):
        raise typer.BadParameter("Directories must be empty.")
    logger.info("Initializing project in %s", root)
    root.joinpath(c.PROJECT_FILE).write_text(
        tomlkit.dumps(
            {
                "workspace": {
                    "members": [str((root / d).relative_to(root)) for d in directories]
                }
            }
        )
    )
    gitignore = root.joinpath(".gitignore")
    with gitignore.open("a") as f:
        f.write("\n".join(["", "*.duckdb", "*.duckdb.wal"]))
    for directory in directories:
        ctx.invoke(init_workspace, directory=root / directory)


@app.command("develop", rich_help_panel="Project Initialization")
def develop(
    ctx: typer.Context,
    component: str = typer.Argument("*", help="The component to develop."),
) -> None:
    """:hammer_and_wrench: Install project components in the active virtual environment."""
    if "VIRTUAL_ENV" not in os.environ:
        raise typer.BadParameter("Must be run in a virtual environment.")
    elif not Path(os.environ["VIRTUAL_ENV"]).is_dir():
        # Sanity check
        raise typer.BadParameter(
            "VIRTUAL_ENV is not a directory. Ensure you are in a valid virtual environment."
        )
    # From here-on, we assume we are in a valid virtual environment and will use the pip module
    project: Project = ctx.obj
    ws_, *component_path = _parse_ws_component(component, project=project)
    ws = project[ws_]
    if not component_path or component_path[0] == "*":
        for comp in ws.pipelines.values():
            comp.install()
        for comp in ws.publishers.values():
            comp.install()
        for comp in ws.scripts.values():
            comp.install()
    elif len(component_path) == 1:
        (typ,) = component_path
        if typ == c.PIPELINES:
            for comp in ws.pipelines.values():
                comp.install()
        elif typ == c.PUBLISHERS:
            for comp in ws.publishers.values():
                comp.install()
        elif typ == c.SCRIPTS:
            for comp in ws.scripts.values():
                comp.install()
    else:
        typ, comp_name = component_path
        if typ == c.PIPELINES:
            comp = ws.pipelines[comp_name].install()
        elif typ == c.PUBLISHERS:
            comp = ws.publishers[comp_name].install()
        elif typ == c.SCRIPTS:
            comp = ws.scripts[comp_name].install()
        else:
            raise typer.BadParameter(
                "Must specify a component in the form <workspace>.<type>.<component>"
            )


def _parse_ws_component(
    component: str, project: "Project | None" = None
) -> t.Tuple[str, ...]:
    """Parse a workspace.component string into a tuple of parts.

    We support the following syntaxes (with all combinations of delimiters)
    workspace.component
    workspace.component.sink
    workspace.component -> sink
    workspace.component >> sink
    workspace.component :: sink
    workspace.component | sink
    workspace >> component >> sink
    workspace/component/sink

    if operating in a project with a default workspace indicating a flat single-tenant structure,
    no workspace should be specified in the component string. Same goes for a single workspace project.

    Args:
        component: The component string to parse.

    Returns:
        A tuple of parts.
    """
    parts = [component]

    # Parse
    while delim := next(
        (d for d in Delimiter if d.value in parts[-1]),
        None,
    ):
        parts.extend(parts.pop(-1).split(delim.value, 1))

    parts = [p.strip() for p in parts]

    # Inject workspace in a single-tenant project
    if project and len(project) == 1:
        ws = next(iter(project))
        if parts[0] != ws:
            parts.insert(0, ws)
    if project and parts[0] not in project:
        raise ValueError(f"Workspace {parts[0]} not found in project.")

    return tuple(parts)


def _print_metadata(metadata: "SupportsComponentMetadata") -> None:
    """
    Print common component metadata.

    Args:
        meta: The component metadata.
    """
    rich.print(f"\n[b]Owners[/b]: [yellow]{metadata.owner}[/yellow]")
    description = metadata.description.replace("\n", " ")
    rich.print(f"[b]Description[/b]: {description}")
    rich.print(f"[b]Tags[/b]: {', '.join(metadata.tags)}")
    if metadata.cron:
        cron = (
            " ".join(metadata.cron.expressions)
            if isinstance(metadata.cron, croniter)
            else metadata.cron
        )
        rich.print(f"[b]Cron[/b]: {cron}\n")


def _metadata_to_md_section(name: str, metadata: "SupportsComponentMetadata") -> str:
    """Convert a component's metadata to a markdown section."""
    md_doc = f"#### {name}\n\n"
    md_doc += f"- **Description**: {metadata.description}\n"
    md_doc += f"- **Owners**: {metadata.owner}\n"
    md_doc += f"- **Tags**: {', '.join(metadata.tags)}\n"
    md_doc += f"- **Cron**: {metadata.cron or 'Not Scheduled'}\n\n"
    return md_doc


if __name__ == "__main__":
    app()
