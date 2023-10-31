"""CLI for cdf."""
import logging
import typing as t
from functools import partial

import dlt
import rich
import typer
from rich.logging import RichHandler

import cdf.core.types as ct
from cdf import get_directory_modules, populate_source_cache
from cdf.core.utils import do, index_destinations

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
SEARCH_PATHS = ["./sources"]

# TODO: Cache population should be a hook on (almost) every command
# The CLI then dictates what sources are evaluated since they are LazySources
# CLI is dynamic based on cache
# Deep FF logic, etc is then hooked into CDFSource


@app.callback()
def main(
    paths: t.List[str] = typer.Option(
        SEARCH_PATHS, "-p", "--path", help="Source directory paths."
    ),
):
    """:sparkles: A [b]framework[b] for managing and running [u]ContinousDataflow[/u] projects. :sparkles:

    [br /]
    - ( :electric_plug: ) [b blue]Sources[/b blue]    are responsible for fetching data from a data source.
    - ( :shuffle_tracks_button: ) [b red]Transforms[/b red] are responsible for transforming data in a data warehouse.
    - ( :mailbox: ) [b yellow]Publishers[/b yellow] are responsible for publishing data to an external system.
    """
    if paths:
        global SEARCH_PATHS
        SEARCH_PATHS = paths
    do(
        lambda path: populate_source_cache(CACHE, partial(get_directory_modules, path)),
        paths or SEARCH_PATHS,
    )


# TODO: move to utils
def _fn_to_str(fn: t.Callable) -> str:
    """Convert a function to a string representation."""
    parts = [
        f"mod: [cyan]{fn.__module__}[/cyan]",
        f"fn: [yellow]{fn.__name__}[/yellow]",
        f"ln: {fn.__code__.co_firstlineno}",
    ]
    return ", ".join(parts)


@app.command()
def index() -> None:
    """:page_with_curl: Print an index of [b blue]Sources[/b blue], [b red]Transforms[/b red], and [b yellow]Publishers[/b yellow] loaded from the source directory paths."""
    rich.print(f"\n Sources Discovered: {len(CACHE)}")
    rich.print(f" Paths Searched: {SEARCH_PATHS}\n")
    rich.print(" [b]Index[/b]")
    for i, (name, fn) in enumerate(CACHE.items(), start=1):
        rich.print(f"  {i}) [b blue]{name}[/b blue] ({_fn_to_str(fn)})")
    rich.print("")


@app.command()
def debug() -> None:
    """:bug: A basic [magenta]debug[/magenta] command."""
    rich.print("Debugging...")


@app.command()
def discover(source: str) -> None:
    """:mag: Evaluates a :zzz: Lazy [b blue]Source[/b blue] and enumerates the discovered resources."""
    if source not in CACHE:
        raise typer.BadParameter(f"Source {source} not found.")
    mod = CACHE[source]()
    rich.print(
        f"\nDiscovered {len(mod.resources)} resources in [b red]{source}[/b red]:"
    )
    for i, resource in enumerate(mod.resources.values(), start=1):
        # TODO: Add feature flag information
        rich.print(f"  {i}) [b blue]{resource.name}[/b blue] (enabled: True)")
    rich.print("")


# TODO: move to utils
def _flatten_stream(it: t.Iterable[T]) -> t.Iterator[T]:
    """Flatten a stream of iterables."""
    for i in it:
        if isinstance(i, list):
            yield from _flatten_stream(i)
        else:
            yield i


@app.command()
def head(
    source: str,
    resource: str,
    num: t.Annotated[int, typer.Option("-n", "--num-rows")] = 5,
) -> None:
    """:wrench: Prints the first N rows of a [b green]Resource[/b green] within a [b blue]Source[/b blue]. Defaults to [cyan]5[/cyan].

    This is useful for quickly inspecting data :detective: and verifying that it is coming over the wire correctly.
    """
    if source not in CACHE:
        raise typer.BadParameter(f"Source {source} not found.")
    mod = CACHE[source]()
    if resource not in mod.resources:
        raise typer.BadParameter(f"Resource {resource} not found in source {source}.")
    r = mod.resources[resource]
    rich.print(f"\nHead of [b red]{resource}[/b red] in [b blue]{source}[/b blue]:")
    mut_num = int(num)
    for row in _flatten_stream(r):
        rich.print(row)
        if mut_num <= 0:
            break
        mut_num -= 1


@app.command(rich_help_panel="Pipelines")
def ingest(
    source: str,
    destination: str = "default",
    resources: t.List[str] = typer.Option(None),
) -> None:
    """:inbox_tray: Ingest data from a [b blue]Source[/b blue] into a data store where it can be [b red]Transformed[/b red]."""
    if source not in CACHE:
        raise typer.BadParameter(f"Source {source} not found.")
    configured_source = CACHE[source]()
    resources = resources or []
    for resource in resources:
        if resource not in configured_source.resources:
            raise typer.BadParameter(
                f"Resource {resource} not found in source {source}."
            )
    if destination not in DESTINATIONS:
        raise typer.BadParameter(
            f"Destination {destination} not found in parsed env vars. Available: {', '.join(DESTINATIONS.keys())}"
        )
    dest = DESTINATIONS[destination]
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


if __name__ == "__main__":
    app()
