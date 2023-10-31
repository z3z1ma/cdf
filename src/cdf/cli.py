"""CLI for cdf."""
import typing as t
from functools import partial

import typer

from cdf import get_directory_modules, populate_source_cache, registry
from cdf.core.utils import do

app = typer.Typer(
    rich_markup_mode="rich",
    epilog="Made with [red]♥[/red] by [bold]z3z1ma[/bold].",
)

CACHE = {}

# TODO: Cache population should be a hook on (almost) every command
# The CLI then dictates what sources are evaluated since they are LazySources
# CLI is dynamic based on cache
# Deep FF logic, etc is then hooked into CDFSource


@app.callback()
def main(
    paths: t.List[str] = typer.Option(
        None, "-p", "--path", help="Source directory paths."
    ),
):
    """:sparkles: A [b]framework[b] for managing and running [u]ContinousDataflow[/u] projects. :sparkles:

    [br /]
    - ( :electric_plug: ) [b blue]Sources[/b blue]    are responsible for fetching data from a data source.
    - ( :shuffle_tracks_button: ) [b red]Transforms[/b red] are responsible for transforming data in a data warehouse.
    - ( :mailbox: ) [b yellow]Publishers[/b yellow] are responsible for publishing data to an external system.
    """
    do(
        lambda path: populate_source_cache(CACHE, partial(get_directory_modules, path)),
        paths or [],
    )


@app.command()
def index() -> None:
    """Print an [blue]index[/blue] of sources loaded from the source directory paths."""
    typer.echo(CACHE)
    for lazy_source in CACHE.values():
        lazy_source()  # side effect of registering source
    for source in registry.get_sources():
        typer.echo(source)


@app.command()
def debug() -> None:
    """A basic debug command."""
    typer.echo("Debugging...")


if __name__ == "__main__":
    app()
