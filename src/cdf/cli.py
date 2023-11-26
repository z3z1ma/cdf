"""CLI for cdf."""
import json
import logging
import os
import subprocess
import sys
import typing as t
from contextlib import suppress
from pathlib import Path

import dlt
import dotenv
import rich
import typer

import cdf.core.constants as c
import cdf.core.logger as logger

if t.TYPE_CHECKING:
    from cdf import Project, publisher_spec, source_spec

T = t.TypeVar("T")

app = typer.Typer(
    rich_markup_mode="rich",
    epilog="Made with [red]♥[/red] by [bold]z3z1ma[/bold].",
    add_completion=False,
    no_args_is_help=True,
)
transform = typer.Typer(no_args_is_help=True)
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
    - ( :electric_plug: ) [b blue]sources[/b blue]    are responsible for fetching data from a data source.
    - ( :shuffle_tracks_button: ) [b red]transforms[/b red] are responsible for transforming data in a data warehouse.
    - ( :mailbox: ) [b yellow]publishers[/b yellow] are responsible for publishing data to an external system.
    """
    from cdf import Project

    logger.set_level(log_level.upper() if not debug else "DEBUG")
    if debug:
        import sqlmesh

        dlt.config["runtime.log_level"] = "DEBUG"
        sqlmesh.configure_logging(force_debug=True)

    ctx.obj = Project.find_nearest(root)
    ctx.obj.meta["root"] = root


@app.command(rich_help_panel="Project Info")
def index(ctx: typer.Context) -> None:
    """:page_with_curl: Print an index of [b][blue]Sources[/blue], [red]Transforms[/red], and [yellow]Publishers[/yellow][/b] loaded from the source directory paths."""
    from cdf.core.utils import fn_to_str

    project: Project = ctx.obj
    rich.print(f" {project}")
    for _, workspace in project:
        rich.print(f"\n ~ {workspace}")
        if not any(workspace.capabilities.values()):
            rich.print(
                f"   No capabilities discovered. Add {c.SOURCES_PATH}, {c.TRANSFORMS_PATH}, or {c.PUBLISHERS_PATH}"
            )
            continue
        if workspace.has_sources:
            rich.print(f"\n   [blue]Sources[/blue]: {len(workspace.sources)}")
            for i, (name, meta) in enumerate(workspace.sources.items(), start=1):
                fn = meta.factory.__wrapped__
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
        md_doc += f"## {workspace.namespace.title()} Space\n\n"
        if workspace.has_dependencies:
            md_doc += "### Dependencies\n\n"
            deps = subprocess.check_output([workspace.pip_path, "freeze"], text=True)
            for dep in deps.splitlines():
                md_doc += f"- `{dep}`\n"
            md_doc += "\n"
        if workspace.has_sources:
            md_doc += "### Sources\n\n"
            for name, meta in workspace.sources.items():
                md_doc += f"### {name}\n\n"
                md_doc += f"**Description**: {meta.description}\n\n"
                md_doc += f"**Owners**: {meta.owners}\n\n"
                md_doc += f"**Tags**: {', '.join(meta.tags)}\n\n"
                md_doc += f"**Cron**: {meta.cron or 'Not Scheduled'}\n\n"
                md_doc += f"**Metrics**: {meta.metrics}\n\n"
            md_doc += "\n"
        if workspace.has_transforms:
            md_doc += "### Transforms\n\n"
        if workspace.has_publishers:
            md_doc += "### Publishers\n\n"
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
    source: t.Annotated[
        str, typer.Argument(help="The <workspace>.<source> to discover.")
    ],
    opts: str = typer.Argument(
        "{}", help="JSON formatted options to forward to the source."
    ),
) -> None:
    """:mag: Evaluates a :zzz: Lazy [b blue]Source[/b blue] and enumerates the discovered resources.

    \f
    Args:
        ctx: The CLI context.
        source: The source to discover.
        opts: JSON formatted options to forward to the source.
    """
    logger.debug("Discovering source %s", source)
    project: Project = ctx.obj
    ws, src = _parse_ws_component(source)
    workspace = project[ws]
    with workspace.runtime_source(src, **json.loads(opts)) as rt_source:
        rich.print(
            f"\nDiscovered {len(rt_source.resources)} resources in"
            f" [b red]{source}.v{workspace[src].version}[/b red]:"
        )
        for i, resource in enumerate(rt_source.resources.values(), start=1):
            if resource.selected:
                rich.print(f"  {i}) [b green]{resource.name}[/b green] (enabled: True)")
            else:
                rich.print(f"  {i}) [b red]{resource.name}[/b red] (enabled: False)")
        _print_meta(workspace[src])


@app.command(rich_help_panel="Inspect")
def head(
    ctx: typer.Context,
    source: t.Annotated[
        str, typer.Argument(help="The <workspace>.<source> to inspect.")
    ],
    resource: str,
    opts: str = typer.Argument(
        "{}", help="JSON formatted options to forward to the source."
    ),
    num: t.Annotated[int, typer.Option("-n", "--num-rows")] = 5,
) -> None:
    """:wrench: Prints the first N rows of a [b green]Resource[/b green] within a [b blue]Source[/b blue]. Defaults to [cyan]5[/cyan].

    This is useful for quickly inspecting data :detective: and verifying that it is coming over the wire correctly.

    \f
    Args:
        ctx: The CLI context.
        source: The source to inspect.
        resource: The resource to inspect.
        opts: JSON formatted options to forward to the source.
        num: The number of rows to print.

    Raises:
        typer.BadParameter: If the resource is not found in the source.
    """
    from cdf.core.utils import flatten_stream

    project: Project = ctx.obj
    ws, src = _parse_ws_component(source)
    workspace = project[ws]
    with workspace.runtime_source(src, **json.loads(opts)) as rt_source:
        if resource not in rt_source.resources:
            raise typer.BadParameter(
                f"Resource {resource} not found in source {source}."
            )
        res = rt_source.resources[resource]
        rich.print(
            f"\nHead of [b red]{resource}[/b red] in [b blue]{source}.v{workspace.version}[/b blue]:"
        )
        it = flatten_stream(res)
        while num > 0 and (v := next(it, None)):  # type: ignore
            rich.print(v)
            v = next(it, None)
            num -= 1


@app.command(rich_help_panel="Integrate")
def ingest(
    ctx: typer.Context,
    source: t.Annotated[
        str, typer.Argument(help="The <workspace>.<source> to ingest.")
    ],
    opts: str = typer.Argument(
        "{}", help="JSON formatted options to forward to the source."
    ),
    dest: t.Annotated[str, typer.Option(..., "-d", "--dest")] = "default",
    resources: t.List[str] = typer.Option(
        ..., "-r", "--resource", default_factory=list
    ),
) -> None:
    """:inbox_tray: Ingest data from a [b blue]Source[/b blue] into a data store where it can be [b red]Transformed[/b red].

    \f
    Args:
        ctx: The CLI context.
        source: The source to ingest from.
        opts: JSON formatted options to forward to the source.
        dest: The destination to ingest to.
        resources: The resources to ingest.

    Raises:
        typer.BadParameter: If no resources are selected.
    """
    project: Project = ctx.obj
    ws, src = _parse_ws_component(source)
    workspace = project[ws]
    with workspace.runtime_source(src, **json.loads(opts)) as rt_source:
        if resources:
            rt_source = rt_source.with_resources(*resources)
        if not rt_source.selected_resources:
            raise typer.BadParameter(
                f"No resources selected for source {source}. Use the discover command to see available resources.\n"
                "Select them explicitly with --resource or enable them with feature flags.\n\n"
                f"Reach out to the source owners for more information: {workspace[src].owners}"
            )
        dataset_name = f"{src}_v{workspace[src].version}"
        rich.print(
            f"Ingesting data from [b blue]{source}[/b blue] to [b red]{dest}[/b red]..."
        )
        for resource in rt_source.selected_resources:
            rich.print(f"  - [b green]{resource}[/b green]")
        engine, dest = dest.split(".", 1)
        pkwargs = {}
        if "BUCKET_URL" in os.environ:
            # Staging native creds use expected cloud provider env vars
            # such as GOOGLE_APPLICATION_CREDENTIALS, AWS_ACCESS_KEY_ID, etc.
            pkwargs["staging"] = "filesystem"
        with suppress(KeyError):
            # Permit credentials to be omitted which will fall back to native parser
            pkwargs["credentials"] = dlt.secrets[f"{engine}.{dest}.credentials"]
        pipeline = dlt.pipeline(
            f"cdf-{src}",
            destination=engine,
            dataset_name=dataset_name,
            progress=os.getenv("CDF_PROGRESS", "alive_progress"),  # type: ignore
            **pkwargs,
        )
        info = pipeline.run(rt_source)
    logging.info(info)


@transform.callback()
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
    ctx.obj = project.get_transform_context(workspaces)


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
    "create_external_models",
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
        return runner(context.fetchdf(runner.query), **json.loads(opts))


# TODO: add metadata command?
# consideration for metadata:
# we want to export dlt schema data to <workspace>/metadata/<destination>/<catalog>/<table>.yaml ?
# dlt schema data does not neecessarily correlate to sourrces and resources and they can
# generate multiple tables or child tables -- metadata is purely related to "tables"
# Our big blocker here I think is that we need to know up front all of our detinations
# which in the current world / implementation is a pain in the ass. We should derive some
# first class support for a "destinations.py" at the top level of a workspace?


# Steps may look like this:
# - ** Add `destinations.py` support **
# - For a CLI requested detination, export schema data from the `pipeline` object to /metadata/_staging
# - Mutate into expected location
# - Run `sqlmesh create_external_models` (from the context) and move the file to /metadata/_staging
# - Mutate into expected location (unified)
# - ** At this point, we can manage unified external models/metadata **
# - SQLMesh metadata consumption
#   - Override CDFTransformLoader `_load_external_models` to consume from /metadata
#     - this needs to know the "destination" name? Suppose we can store it since we subclass anyway
#     - we are brushing up against a larger convergence here, maybe destinations.py can solve for this but its... hard
# - DLT metadata consumption
#   - dlt should probably consume interesting user overrides from these yaml files?
#     but I cannot find the fucking answer on if that disables schema evolution...
# - ** At this point, components can leverage unified metadata **
# - Now we should support our custom DSL for "staging" models created via `cdf generate-staging-layer`, sick...
# - So publishers should be able to be based on a model, and we can use sqlmesh evaluate to get the model as a dataframe
#   or think through a lazier way to get the data, but just one-shotting a pandas dataframe is reasonable here to me
#   in a first-pass since most `publish` operations are not massive ops, and who knows maybe pandas will buffer to disk
#   if we do some legwork to research it? Though SQLMesh likely makes it eager? Surely they have some lazy interface.

# I wonder if a workspace should have a "primary" destination?
# Consideration for `cdf metadata`, otherwise user must specify destination everytime they run this dump command
# We can manage the idea of a default destination inside the destinations.py
# A py file coincidentally might align with dlt 0.4.0 approach, our file will return config -- post 0.4.0 it will return
# actual destination objects


@app.command(rich_help_panel="Utility")
def run(
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
    "bin",
    rich_help_panel="Utility",
    context_settings={"allow_extra_args": True, "ignore_unknown_options": True},
)
def bin_(ctx: typer.Context, executable: str) -> None:
    """:rocket: Run an executable located in a workspace environment.

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


def _parse_ws_component(component: str) -> t.Tuple[str, str]:
    """Parse a workspace.component string into a tuple.

    Args:
        component: The component string to parse.

    Returns:
        A tuple of (workspace, component).
    """
    if "." in component:
        ws, src = component.split(".", 1)
        return ws, src
    return c.DEFAULT_WORKSPACE, component


def _print_meta(meta: "source_spec | publisher_spec") -> None:
    """Print common component metadata.

    Args:
        meta: The source metadata.
    """
    rich.print(f"\nOwners: [yellow]{meta.owners}[/yellow]")
    rich.print(f"Description: {meta.description}")
    rich.print(f"Tags: {meta.tags}")
    rich.print(f"Cron: {meta.cron}\n")


if __name__ == "__main__":
    app()
