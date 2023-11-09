"""CLI for cdf."""
import logging
import os
import typing as t
from functools import partial
from pathlib import Path

import dlt
import dotenv
import rich
import typer
from rich.logging import RichHandler

import cdf.core.constants as c
import cdf.core.feature_flags as ff
import cdf.core.types as ct
from cdf import (
    CDFSource,
    CDFSourceMeta,
    cdf_logger,
    get_directory_modules,
    populate_source_cache,
)
from cdf.core.config import add_providers_from_workspace
from cdf.core.utils import (
    flatten_stream,
    fn_to_str,
    index_destinations,
    parse_workspace_member,
    read_workspace_file,
)

T = t.TypeVar("T")

logging.basicConfig(
    level="INFO",
    format="%(message)s",
    datefmt="[%X]",
    handlers=[RichHandler(level="INFO")],
)

app = typer.Typer(
    rich_markup_mode="rich",
    epilog="Made with [red]♥[/red] by [bold]z3z1ma[/bold].",
    add_completion=False,
)

dotenv.load_dotenv()

CACHE: ct.SourceSpec = {}
DESTINATIONS: ct.DestinationSpec = {}


@app.callback()
def main(
    ctx: typer.Context,
    root: Path = typer.Option(
        ...,
        "-p",
        "--path",
        default_factory=Path.cwd,
        help="Path to the project root. Defaults to cwd. Parent dirs are searched for a workspace file.",
    ),
    log_level: str = typer.Option(
        "INFO",
        "-l",
        "--log-level",
        help="Set the log level. Defaults to INFO.",
        envvar="CDF_LOG_LEVEL",
    ),
):
    """:sparkles: a [b]framework[b] for managing and running [u]continousdataflow[/u] projects. :sparkles:

    [br /]
    - ( :electric_plug: ) [b blue]sources[/b blue]    are responsible for fetching data from a data source.
    - ( :shuffle_tracks_button: ) [b red]transforms[/b red] are responsible for transforming data in a data warehouse.
    - ( :mailbox: ) [b yellow]publishers[/b yellow] are responsible for publishing data to an external system.
    """
    # Set log level
    cdf_logger.set_level(log_level)

    # Load workspaces
    workspaces: t.Dict[str, Path] = {}
    workspace, fpath = read_workspace_file(root)
    if workspace and fpath:
        cdf_logger.debug("Found workspace file %s, using multi-project layout", fpath)
        add_providers_from_workspace("__cdf_root__", fpath)
        for member in workspace["members"]:
            wname, wpath = parse_workspace_member(member)
            workspaces.update({wname: fpath / wpath})
    else:
        cdf_logger.debug(
            "No workspace file found in %s, using single-project layout", root
        )
        fpath = root.expanduser().resolve()
        workspaces[c.DEFAULT_WORKSPACE] = fpath

    # Load root .env
    if dotenv.load_dotenv(dotenv_path=fpath / ".env"):
        cdf_logger.debug("Loaded .env file from %s", fpath)

    # Load workspace sources
    for workspace_name, workspace_path in workspaces.items():
        cdf_logger.debug("Loading workspace %s from %s", workspace_name, workspace_path)

        # Do workspace .env
        if dotenv.load_dotenv(dotenv_path=workspace_path / ".env"):
            cdf_logger.debug("Loaded .env file from %s", workspace_path)

        # Do sources
        populate_source_cache(
            CACHE,
            get_modules_fn=partial(
                get_directory_modules, workspace_path / c.SOURCES_PATH
            ),
            namespace=workspace_name,
        )

        # Do SQLMesh
        ...

        # Do publishers
        ...

    # Do destinations, TODO: better ways to do this, was just for POC
    DESTINATIONS.update(index_destinations())

    # Capture workspaces in the CLI context
    ctx.obj = workspaces


def _inject_config_for_source(source: str, ctx: typer.Context) -> str:
    """Inject config into the CLI context.

    The order of precedence is workspace config, the root config

    Args:
        source: The source to inject config for.
        ctx: The CLI context.
    """
    workspaces = ctx.obj
    if c.DEFAULT_WORKSPACE in workspaces:
        add_providers_from_workspace(
            c.DEFAULT_WORKSPACE, workspaces[c.DEFAULT_WORKSPACE]
        )
    if "." in source:
        workspace, _ = source.split(".", 1)
        if workspace not in workspaces:
            raise typer.BadParameter(f"Workspace {workspace} not found.")
        add_providers_from_workspace(workspace, workspaces[workspace])
    return source


@app.command()
def index() -> None:
    """:page_with_curl: Print an index of [b blue]Sources[/b blue], [b red]Transforms[/b red], and [b yellow]Publishers[/b yellow] loaded from the source directory paths."""
    _print_sources()
    _print_destinations()
    rich.print("")


@app.command()
def debug() -> None:
    """:bug: A basic [magenta]debug[/magenta] command."""
    rich.print("Debugging...")


@app.command()
def discover(
    ctx: typer.Context,
    source: t.Annotated[str, typer.Argument(callback=_inject_config_for_source)],
) -> None:
    """:mag: Evaluates a :zzz: Lazy [b blue]Source[/b blue] and enumerates the discovered resources."""
    cdf_logger.debug("Discovering source %s", source)
    mod, meta = _get_source(source, ctx.obj)
    rich.print(
        f"\nDiscovered {len(mod.resources)} resources in [b red]{source}.v{meta.version}[/b red]:"
    )
    for i, resource in enumerate(mod.resources.values(), start=1):
        if resource.selected:
            rich.print(f"  {i}) [b green]{resource.name}[/b green] (enabled: True)")
        else:
            rich.print(f"  {i}) [b red]{resource.name}[/b red] (enabled: False)")
    rich.print(f"\nOwners: [yellow]{meta.owners}[/yellow]\n")


@app.command()
def head(
    ctx: typer.Context,
    source: t.Annotated[str, typer.Argument(callback=_inject_config_for_source)],
    resource: str,
    num: t.Annotated[int, typer.Option("-n", "--num-rows")] = 5,
) -> None:
    """:wrench: Prints the first N rows of a [b green]Resource[/b green] within a [b blue]Source[/b blue]. Defaults to [cyan]5[/cyan].

    This is useful for quickly inspecting data :detective: and verifying that it is coming over the wire correctly.
    """
    src, meta = _get_source(source, ctx.obj)
    res = _get_resource(src, resource)
    rich.print(
        f"\nHead of [b red]{resource}[/b red] in [b blue]{source}.v{meta.version}[/b blue]:"
    )
    it = flatten_stream(res)
    v = next(it, None)
    while num > 0 and v:
        rich.print(v)
        v = next(it, None)
        num -= 1


@app.command(rich_help_panel="Pipelines")
def ingest(
    ctx: typer.Context,
    source: t.Annotated[str, typer.Argument(callback=_inject_config_for_source)],
    destination: t.Annotated[str, typer.Option(..., "-d", "--dest")] = "default",
    resources: t.List[str] = typer.Option(
        ..., "-r", "--resource", default_factory=list
    ),
) -> None:
    """:inbox_tray: Ingest data from a [b blue]Source[/b blue] into a data store where it can be [b red]Transformed[/b red]."""
    configured_source, meta = _get_source(source, ctx.obj)
    if resources:
        configured_source = configured_source.with_resources(*resources)
    if not configured_source.selected_resources:
        raise typer.BadParameter(
            f"No resources selected for source {source}. Use the discover command to see available resources."
            "\nSelect them explicitly with --resource or enable them with feature flags."
            f"\nReach out to the source owners for more information: {meta.owners}"
        )
    dest = _get_destination(destination)
    rich.print(
        f"Ingesting data from [b blue]{source}[/b blue] to [b red]{dest.engine}[/b red]..."
    )
    for resource in configured_source.selected_resources:
        rich.print(f"  - [b green]{resource}[/b green]")
    if "." in source:
        _, source = source.split(".", 1)
    pipeline = dlt.pipeline(
        f"{source}-to-{destination}",
        destination=dest.engine,
        credentials=dest.credentials,
        # TODO: set staging?
        dataset_name=f"{source}_v{meta.version}",
        progress="alive_progress",
    )
    info = pipeline.run(configured_source)
    logging.info(info)


@app.command(rich_help_panel="Pipelines")
def transform() -> None:
    """:arrows_counterclockwise: [b red]Transform[/b red] data from a data store into a data store where it can be exposed or [b yellow]Published[/b yellow]."""
    rich.print("Transforming with SQLMesh...")


@app.command(rich_help_panel="Pipelines")
def publish() -> None:
    """:outbox_tray: [b yellow]Publish[/b yellow] data from a data store to an [violet]External[/violet] system."""
    rich.print("Publishing...")


def _get_source(
    source: str, workspaces: t.Dict[str, Path]
) -> t.Tuple[CDFSource, CDFSourceMeta]:
    """Get a source from the global cache. This will also apply feature flags.

    Args:
        source: The name of the source to get.

    Raises:
        typer.BadParameter: If the source is not found.

    Returns:
        The source.
    """
    if source not in CACHE:
        raise typer.BadParameter(f"Source {source} not found.")
    meta = CACHE[source]
    cdf_logger.debug("Loading source %s", source)
    mod = meta.deferred_fn()
    if "." in source:
        workspace, source = source.split(".", 1)
    else:
        workspace = c.DEFAULT_WORKSPACE
    mod.name = source
    cmp_ffs = ff.get_source_flags(
        mod, workspace_name=workspace, workspace_path=workspaces[workspace]
    )
    ff.apply_feature_flags(mod, cmp_ffs, workspace)
    return mod, meta


def _get_resource(source: CDFSource, resource: str) -> dlt.sources.DltResource:  # type: ignore
    """Get a resource from a source.

    Args:
        source: The source to get the resource from.
        resource: The name of the resource to get.

    Raises:
        typer.BadParameter: If the resource is not found.

    Returns:
        The resource.
    """
    if resource not in source.resources:
        raise typer.BadParameter(f"Resource {resource} not found in source {source}.")
    return source.resources[resource]


def _get_destination(destination: str) -> ct.EngineCredentials:
    """Get a destination from the global cache.

    Args:
        destination: The name of the destination to get.

    Raises:
        typer.BadParameter: If the destination is not found.

    Returns:
        The destination.
    """
    if destination not in DESTINATIONS:
        raise typer.BadParameter(f"Destination {destination} not found.")
    return DESTINATIONS[destination]


def _print_sources() -> None:
    """Print the source index in the global cache."""
    rich.print(f"\n Sources Discovered: {len(CACHE)}")
    rich.print(f" Paths Searched: {c.COMPONENT_PATHS}\n")
    rich.print(" [b]Index[/b]")
    for i, (name, meta) in enumerate(CACHE.items(), start=1):
        fn = meta.deferred_fn
        rich.print(f"  {i}) [b blue]{name}[/b blue] ({fn_to_str(fn)})")


def _print_destinations() -> None:
    """Print the destination index in the global cache."""
    rich.print(f"\n Destinations Discovered: {len(DESTINATIONS)}")
    rich.print(f" Env Vars Parsed: {[e for e in os.environ if e.startswith('CDF_')]}\n")
    rich.print(" [b]Index[/b]")
    for i, (name, creds) in enumerate(DESTINATIONS.items(), start=1):
        rich.print(f"  {i}) [b blue]{name}[/b blue] (engine: {creds.engine})")


if __name__ == "__main__":
    app()
