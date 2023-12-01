"""CLI for cdf."""
import json
import os
import subprocess
import sys
import tempfile
import typing as t
from pathlib import Path

import dlt
import dotenv
import rich
import typer

import cdf.core.constants as c
import cdf.core.logger as logger

if t.TYPE_CHECKING:
    from cdf import Project, pipeline_spec, publisher_spec

T = t.TypeVar("T")

app = typer.Typer(
    rich_markup_mode="rich",
    epilog="Made with [red]♥[/red] by [bold]z3z1ma[/bold].",
    add_completion=False,
    no_args_is_help=True,
)
transform = typer.Typer(
    rich_markup_mode="rich",
    epilog="Made with [red]♥[/red] by [bold]z3z1ma[/bold].",
    add_completion=False,
    no_args_is_help=True,
)
app.add_typer(transform, name="transform", rich_help_panel="Integrate")

dotenv.load_dotenv()


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
            ..., "-d", "--debug", help="Run in debug mode, force log level to debug"
        ),
    ] = False,
):
    """:sparkles: a [b]framework[b] for managing and running [u]continousdataflow[/u] projects. :sparkles:

    [b/]
    - ( :electric_plug: ) [b blue]pipelines[/b blue]    are responsible for fetching data from a data pipeline.
    - ( :shuffle_tracks_button: ) [b red]transforms[/b red] are responsible for transforming data in a data warehouse.
    - ( :mailbox: ) [b yellow]publishers[/b yellow] are responsible for publishing data to an external system.
    """
    from cdf import Project

    logger.set_level(log_level.upper() if not debug else "DEBUG")
    if debug:
        import sqlmesh

        dlt.config["runtime.log_level"] = "DEBUG"
        sqlmesh.configure_logging(force_debug=True)

    if ctx.invoked_subcommand in ("init-project", "init-workspace"):
        return

    ctx.obj = Project.find_nearest(root)
    ctx.obj.meta["root"] = root


@app.command(rich_help_panel="Project Info")
def index(ctx: typer.Context) -> None:
    """:page_with_curl: Print an index of [b][blue]Pipelines[/blue], [red]Transforms[/red], and [yellow]Publishers[/yellow][/b] loaded from the pipeline directory paths."""
    from cdf.core.utils import fn_to_str

    project: Project = ctx.obj
    rich.print(f" {project}")
    for _, workspace in project:
        rich.print(f"\n ~ {workspace}")
        if not any(workspace.capabilities.values()):
            rich.print(
                f"   No capabilities discovered. Add {c.PIPELINES_PATH}, {c.TRANSFORMS_PATH}, or {c.PUBLISHERS_PATH}"
            )
            continue
        if workspace.has_pipelines:
            rich.print(f"\n   [blue]Pipelines[/blue]: {len(workspace.pipelines)}")
            for i, (name, meta) in enumerate(workspace.pipelines.items(), start=1):
                fn = meta.run.__wrapped__
                rich.print(f"   {i}) {name} ({fn_to_str(fn)})")
        if workspace.has_transforms:
            rich.print(f"\n   [red]Transforms[/red]: {len(workspace.transforms)}")
            for i, (name, _) in enumerate(workspace.transforms.items(), start=1):
                rich.print(f"   {i}) {name}")
        if workspace.has_publishers:
            rich.print(f"\n   [yellow]Publishers[/yellow]: {len(workspace.publishers)}")
            for i, (name, meta) in enumerate(workspace.publishers.items(), start=1):
                fn = meta.runner.__wrapped__
                rich.print(f"   {i}) {name} ({fn_to_str(fn)})")
        if workspace.has_dependencies:
            deps = [
                dep.split("#", 1)[0].strip()  # Basic requirements.txt parsing
                for dep in workspace.requirements_path.read_text().splitlines()
                if dep and not dep.startswith("#")
            ]
            rich.print(f"\n   [green]Dependencies[/green]: {len(deps)}")
            for i, dep in enumerate(deps, start=1):
                rich.print(f"   {i}) {dep}")
    rich.print("")


@app.command(rich_help_panel="Project Info")
def docs(ctx: typer.Context) -> None:
    """:book: Render documentation for the project."""
    project: Project = ctx.obj
    docs_path = project.meta["root"].joinpath("docs")
    if not docs_path.exists():
        docs_path.mkdir()
    md_doc = "# CDF Project\n\n"
    for _, workspace in project:
        md_doc += f"## ✨ {workspace.namespace.title()} Space\n\n"
        if workspace.has_dependencies:
            md_doc += "### 🧱 Dependencies\n\n"
            deps = subprocess.check_output([workspace.pip_path, "freeze"], text=True)
            for dep in deps.splitlines():
                md_doc += f"- `{dep}`\n"
            md_doc += "\n"
        if workspace.has_pipelines:
            md_doc += "### 🚚 Pipelines\n\n"
            for name, meta in workspace.pipelines.items():
                md_doc += f"#### {name}\n\n"
                md_doc += f"**Description**: {meta.description}\n\n"
                md_doc += f"**Owners**: {meta.owners}\n\n"
                md_doc += f"**Tags**: {', '.join(meta.tags)}\n\n"
                md_doc += f"**Cron**: {meta.cron or 'Not Scheduled'}\n\n"
            md_doc += "\n"
        if workspace.has_transforms:
            md_doc += "### 🔄 Transforms\n\n"
            for name, meta in workspace.transforms.items():
                md_doc += f"#### {name}\n\n"
                md_doc += f"**Description**: {meta.description}\n\n"
                md_doc += f"**Owner**: {meta.owner}\n\n"
                md_doc += f"**Tags**: {', '.join(meta.tags)}\n\n"
                md_doc += f"**Cron**: {meta.cron or 'Not Scheduled'}\n\n"
        if workspace.has_publishers:
            md_doc += "### 🖋️ Publishers\n\n"
            for name, meta in workspace.publishers.items():
                md_doc += f"#### {name}\n\n"
                md_doc += f"**Description**: {meta.description}\n\n"
                md_doc += f"**Owners**: {meta.owners}\n\n"
                md_doc += f"**Tags**: {', '.join(meta.tags)}\n\n"
                md_doc += f"**Cron**: {meta.cron or 'Not Scheduled'}\n\n"
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
        print(project.meta["root"].absolute(), file=sys.stdout, flush=True)


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
    ws, src = _parse_ws_component(pipeline)
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
        _print_meta(workspace.pipelines[src])


@app.command(rich_help_panel="Inspect")
def head(
    ctx: typer.Context,
    pipeline: t.Annotated[
        str, typer.Argument(help="The <workspace>.<pipeline> to inspect.")
    ],
    resource: str,
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
    ws, src = _parse_ws_component(pipeline)
    workspace = project[ws]
    with workspace.runtime_source(src, **json.loads(opts)) as rt_source:
        if resource not in rt_source.resources:
            raise typer.BadParameter(
                f"Resource {resource} not found in source for pipeline {pipeline}."
            )
        res = rt_source.resources[resource]
        rich.print(
            f"\nHead of [b red]{resource}[/b red] in [b blue]{pipeline}.v{workspace.pipelines[src].version}[/b blue]:"
        )
        it = flatten_stream(res)
        while num > 0 and (v := next(it, None)):  # type: ignore
            rich.print(v)
            v = next(it, None)
            num -= 1


@app.command("pipeline", rich_help_panel="Integrate")
def run_pipeline(
    ctx: typer.Context,
    pipeline: t.Annotated[
        str, typer.Argument(help="The <workspace>.<pipeline> to ingest.")
    ],
    sink: t.Annotated[str, typer.Option(..., "-s", "--sink")] = "default",
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
        pipeline: The pipeline to ingest from.
        opts: JSON formatted options to forward to the pipeline.
        sink: The destination to ingest to.
        resources: The resources to ingest.

    Raises:
        typer.BadParameter: If no resources are selected.
    """
    project: Project = ctx.obj
    ws, src = _parse_ws_component(pipeline)
    workspace = project[ws]
    with workspace.overlay():
        pipe = workspace.pipelines[src]
        info = pipe.run(workspace, sink, resources, **json.loads(opts))
    logger.info(info)
    if pipe.runtime_metrics:
        logger.info("Runtime Metrics:")
        logger.info(pipe.runtime_metrics)


@transform.callback(invoke_without_command=True)
def transform_entrypoint(
    ctx: typer.Context,
    workspace: t.Annotated[
        str,
        typer.Argument(
            help="A comma separated list of 1 or more workspaces to include in the context. The first workspace is the primary workspace."
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
        if workspace in SQLMESH_COMMANDS:
            raise typer.BadParameter(
                f"When running a {workspace} command, you must specify a workspace."
                f" For example: cdf transform {next(iter(project.keys()))} {workspace}"
            )
        elif workspace in project:
            ctx.invoke(transform, ["--help"])
        else:
            raise typer.BadParameter(
                f"Workspace `{workspace}` not found. Available workspaces: {', '.join(project.keys())}"
            )
    if "." in workspace:
        workspace, sink = _parse_ws_component(workspace)
    else:
        sink = None
    workspaces = workspace.split(",")
    main_workspace = workspaces[0]
    # Ensure we have a primary workspace
    if main_workspace == "*":
        raise typer.BadParameter(
            "Cannot run transforms without a primary workspace. Specify a workspace in the first position."
        )
    # A special case for running a plan with all workspaces accessible to the context
    if any(ws == "*" for ws in workspaces):
        others = project.keys().difference(main_workspace)
        workspaces = [main_workspace, *others]
    # Ensure all workspaces exist and are valid
    for ws in workspaces:
        if ws not in project:
            raise typer.BadParameter(f"Workspace `{ws}` not found.")
        if not project[ws].has_transforms:
            raise typer.BadParameter(
                f"No transforms discovered in workspace `{ws}`. Add transforms to {c.TRANSFORMS_PATH} to enable them."
            )
    # Swap context to SQLMesh context
    ctx.obj = project.get_transform_context(workspaces, sink=sink)


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


def _get_transform_command_wrapper(name: str):
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
        parser = cmd.make_parser(ctx)
        opts, args, _ = parser.parse_args(ctx.args)
        return ctx.invoke(cmd, *args, **opts)

    _passthrough.__name__ = name
    _passthrough.__doc__ = f"{doc} See the CLI reference for options: https://sqlmesh.readthedocs.io/en/stable/reference/cli/#{name}"
    return _passthrough


for passthrough in SQLMESH_COMMANDS:
    transform.command(
        passthrough,
        context_settings={"allow_extra_args": True, "ignore_unknown_options": True},
    )(_get_transform_command_wrapper(passthrough))


@app.command(rich_help_panel="Integrate")
def publish(
    ctx: typer.Context,
    publisher: t.Annotated[
        str, typer.Argument(help="the <workspace>.<publisher> to run")
    ],
    opts: str = typer.Argument(
        "{}", help="JSON formatted options to forward to the publisher."
    ),
) -> None:
    """:outbox_tray: [b yellow]Publish[/b yellow] data from a data store to an [violet]External[/violet] system."""
    from cdf.core.publisher import Payload

    project: Project = ctx.obj
    ws, pub = _parse_ws_component(publisher)
    workspace = project[ws]
    with workspace.overlay():
        runner = workspace.publishers[pub]
        context = workspace.get_transform_context()
        if runner.from_model not in context.models:
            raise typer.BadParameter(
                f"Model {runner.from_model} not found in transform context."
            )
        runner(data=Payload(context.fetchdf(runner.query)), **json.loads(opts))


@app.command("execute-script", rich_help_panel="Utility")
def run_script(
    ctx: typer.Context,
    script: t.Annotated[str, typer.Argument(help="The <workspace>.<script> to run")],
    opts: str = typer.Argument(
        "{}", help="JSON formatted options to forward to the publisher."
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
    """
    from cdf.core.utils import load_module_from_path

    project: Project = ctx.obj
    ws, script = _parse_ws_component(script)
    workspace = project[ws]
    with workspace.overlay():
        mod, _ = load_module_from_path(workspace.get_script(script, must_exist=True))
        mod.entrypoint(workspace, **json.loads(opts))


@app.command(
    "execute-bin",
    rich_help_panel="Utility",
    context_settings={"allow_extra_args": True, "ignore_unknown_options": True},
)
def run_bin(ctx: typer.Context, executable: str) -> None:
    """:rocket: Run an executable located in a workspace venv bin directory.

    This is convenient for running package scripts installed in a workspace environment without having to specify the
    full path to the executable. It is purely a convenience method and is not required to operate within a CDF
    project. All arguments should be passed after a -- separator.

    \f
    Example:
        cdf bin my_workspace.pip -- --help
        cdf bin my_workspace.gcloud -- --help

    Args:
        ctx: The CLI context.
        executable: The executable to run. <workspace>.<executable>

    Raises:
        subprocess.CalledProcessError: If the executable returns a non-zero exit code.
    """
    project: Project = ctx.obj
    ws, comp = _parse_ws_component(executable)
    rich.print(">>> Running", ws, comp, file=sys.stderr)
    workspace = project[ws]
    with workspace.overlay():
        proc = subprocess.run([workspace.get_bin(comp, must_exist=True), *ctx.args])
    raise typer.Exit(proc.returncode)


@app.command("fetch-metadata", rich_help_panel="Utility")
def metadata(ctx: typer.Context, workspace: str) -> None:
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
    from sqlglot import exp, parse_one

    logger.info("Fetching and aggregating metadata from sqlmesh and dlt")
    project: Project = ctx.obj
    yaml = YAML(typ="rt")

    ws = project[workspace]
    with ws.overlay():
        context = ws.get_transform_context()
        schema_out = ws.root / "schema.yaml"
        schema_out.unlink(missing_ok=True)

        context.create_external_models()
        meta = yaml.load(schema_out.read_text()) or []
        schema_out.unlink(missing_ok=True)

        output = {}
        for entry in meta:
            table = parse_one(entry["name"], into=exp.Table)
            catalog = output.setdefault(table.db, {})
            columns = [{"data_type": c} for c in entry["columns"]]
            entry["columns"] = columns
            catalog[table.name] = entry
        for name, src in ws.pipelines.items():
            d = tempfile.TemporaryDirectory()
            dataset = f"{name}_v{src.version}"
            pipe = dlt.pipeline(
                f"cdf-{name}",
                dataset_name=dataset,
                **ws.prod_sink[1].ingest,
                pipelines_dir=d.name,
            )
            pipe.activate()
            pipe.sync_destination()
            for schema in pipe.schemas.values():
                for meta in schema.data_tables():
                    assert meta["name"]
                    table = parse_one(meta["name"], into=exp.Table)
                    catalog = output.setdefault(dataset, {})
                    catalog.setdefault(table.name, {}).update(meta)
            d.cleanup()

    meta_path = ws.root / "metadata"
    meta_path.mkdir(exist_ok=True)
    for catalog, tables in output.items():
        with meta_path.joinpath(f"{catalog}.yaml").open("w") as f:
            yaml.dump(tables, f)


@app.command("generate-staging-layer", rich_help_panel="Utility")
def generate_staging_layer(
    ctx: typer.Context,
    workspace: str,
    fetch_metadata: bool = typer.Option(
        True, help="Regenerate metadata before running"
    ),
) -> None:
    """:floppy_disk: Generate a staging layer for a catalog.

    After fetching metadata, this will generate a staging layer for each catalog. This is typically
    followed by cdf transform plan to materialize the staging layers.

    \f
    Args:
        ctx: The CLI context.
        workspace: The workspace to generate staging layers for.
        fetch_metadata: Whether to fetch metadata before generating staging layers.
    """
    from ruamel.yaml import YAML
    from sqlglot import exp, parse_one

    if fetch_metadata:
        metadata(ctx, workspace)

    logger.info("Generating cdf DSL staging layer")
    project: Project = ctx.obj
    yaml = YAML(typ="rt")

    ws = project[workspace]
    context = ws.get_transform_context()
    for fp in (ws.root / "metadata").iterdir():
        with fp.open() as fd:
            meta = yaml.load(fd)
        for table, meta in meta.items():
            # Check if the output table already exists in the context
            output = f"cdf_staging.stg_{fp.stem}__{table}"
            if output in context.models:
                logger.debug("Skipping %s, already exists", output)
                continue
            # Generate the DSL for the new table and write it to the staging layer
            logger.info("Generating %s", output)
            new_table = parse_one(f"{fp.stem}.{table}", into=exp.Table)
            p = ws.transform_path / "staging" / new_table.db / f"{new_table.name}.yaml"
            p.parent.mkdir(parents=True, exist_ok=True)
            with p.open("w") as f:
                yaml.dump(
                    {
                        "input": f"{new_table.db}.{new_table.name}",
                        "prefix": "",
                        "suffix": "",
                        "excludes": [],
                        "exclude_patterns": [],
                        "includes": [],
                        "include_patterns": [],
                        "predicate": "",
                        "computed_columns": [],
                    },
                    f,
                )


@app.command("init-workspace", rich_help_panel="Utility")
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
    if any(os.listdir(directory)):
        raise typer.BadParameter("Directory must be empty.")
    logger.info("Initializing workspace in %s", directory)
    for dir_ in c.DIR_LAYOUT:
        directory.joinpath(dir_).mkdir(parents=True, exist_ok=False)
    directory.joinpath(c.CONFIG_FILE).touch()
    directory.joinpath(".env").touch()
    directory.joinpath(".gitignore").touch()
    directory.joinpath("requirements.txt").touch()


@app.command("init-project", rich_help_panel="Utility")
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
    if any(os.listdir(d) for d in directories):
        raise typer.BadParameter("Directories must be empty.")
    if any(d.is_absolute() for d in directories):
        raise typer.BadParameter("Directories must be relative paths.")
    root.joinpath(c.WORKSPACE_FILE).write_text(
        tomlkit.dumps({"workspace": [str(d.relative_to(root)) for d in directories]})
    )
    for directory in directories:
        ctx.invoke(init_workspace, directory=root / directory)


def _parse_ws_component(component: str) -> t.Tuple[str, str]:
    """Parse a workspace.component string into a tuple.

    Args:
        component: The component string to parse.

    Returns:
        A tuple of (workspace, component).
    """
    if "." in component:
        ws, comp = component.split(".", 1)
        return ws, comp
    return c.DEFAULT_WORKSPACE, component


def _print_meta(meta: "pipeline_spec | publisher_spec") -> None:
    """Print common component metadata.

    Args:
        meta: The component metadata.
    """
    rich.print(f"\nOwners: [yellow]{meta.owners}[/yellow]")
    rich.print(f"Description: {meta.description}")
    rich.print(f"Tags: {meta.tags}")
    rich.print(f"Cron: {meta.cron}\n")


if __name__ == "__main__":
    app()
