"""CLI for cdf."""
import logging
import os
import typing as t
from functools import partial

import dlt
import rich
import typer
from rich.logging import RichHandler

import cdf.core.constants as c
import cdf.core.types as ct
from cdf import CDFSource, get_directory_modules, populate_source_cache
from cdf.core.config import extend_global_providers, get_config_providers
from cdf.core.utils import do, flatten_stream, fn_to_str, index_destinations

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

CACHE: ct.SourceSpec = {}
DESTINATIONS: ct.DestinationSpec = index_destinations()


@app.callback()
def main(
    paths: t.List[str] = typer.Option(
        ..., "-p", "--path", default_factory=list, help="Source directory paths."
    ),
):
    """:sparkles: A [b]framework[b] for managing and running [u]ContinousDataflow[/u] projects. :sparkles:

    [br /]
    - ( :electric_plug: ) [b blue]Sources[/b blue]    are responsible for fetching data from a data source.
    - ( :shuffle_tracks_button: ) [b red]Transforms[/b red] are responsible for transforming data in a data warehouse.
    - ( :mailbox: ) [b yellow]Publishers[/b yellow] are responsible for publishing data to an external system.
    """
    c.COMPONENT_PATHS.extend(paths)
    do(
        lambda path: populate_source_cache(CACHE, partial(get_directory_modules, path)),
        paths or c.COMPONENT_PATHS,
    )
    extend_global_providers(get_config_providers(c.COMPONENT_PATHS))


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
def discover(source: str) -> None:
    """:mag: Evaluates a :zzz: Lazy [b blue]Source[/b blue] and enumerates the discovered resources."""
    mod = _get_source(source)
    rich.print(
        f"\nDiscovered {len(mod.resources)} resources in [b red]{source}[/b red]:"
    )
    for i, resource in enumerate(mod.resources.values(), start=1):
        if mod.resource_flag_enabled(resource.name):
            rich.print(f"  {i}) [b green]{resource.name}[/b green] (enabled: True)")
        else:
            rich.print(f"  {i}) [b red]{resource.name}[/b red] (enabled: False)")
    rich.print("")


@app.command()
def head(
    source: str,
    resource: str,
    num: t.Annotated[int, typer.Option("-n", "--num-rows")] = 5,
) -> None:
    """:wrench: Prints the first N rows of a [b green]Resource[/b green] within a [b blue]Source[/b blue]. Defaults to [cyan]5[/cyan].

    This is useful for quickly inspecting data :detective: and verifying that it is coming over the wire correctly.
    """
    mod = _get_source(source)
    r = _get_resource(mod, resource)
    rich.print(f"\nHead of [b red]{resource}[/b red] in [b blue]{source}[/b blue]:")
    mut_num = int(num)
    for row in flatten_stream(r):
        rich.print(row)
        if mut_num <= 0:
            break
        mut_num -= 1


@app.command(rich_help_panel="Pipelines")
def ingest(
    source: str,
    destination: str = "default",
    resources: t.List[str] = typer.Option(..., default_factory=list),
) -> None:
    """:inbox_tray: Ingest data from a [b blue]Source[/b blue] into a data store where it can be [b red]Transformed[/b red]."""
    configured_source = _get_source(source)
    if resources:
        configured_source = configured_source.with_resources(*resources)
    dest = _get_destination(destination)
    rich.print(
        f"Ingesting data from [b blue]{source}[/b blue] to [b red]{dest.engine}[/b red]..."
    )
    for resource in configured_source.selected_resources:
        rich.print(f"  - [b green]{resource}[/b green]")
    pipeline = dlt.pipeline(
        f"{source}-to-{destination}",
        destination=dest.engine,
        credentials=dest.credentials,
        # TODO: set staging?
        # Also capture more metadata like "version" for sources to keep our concatenated naming schema
        dataset_name=source,
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


def _get_source(source: str) -> CDFSource:
    """Get a source from the global cache.

    Args:
        source: The name of the source to get.

    Raises:
        typer.BadParameter: If the source is not found.

    Returns:
        The source.
    """
    if source not in CACHE:
        raise typer.BadParameter(f"Source {source} not found.")
    mod = CACHE[source]()
    mod.setup(alias=source)
    return mod


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
    for i, (name, fn) in enumerate(CACHE.items(), start=1):
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
