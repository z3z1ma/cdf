"""CLI for cdf."""
import typing as t

import typer

from cdf import get_directory_modules, populate_source_cache, registry

app = typer.Typer()


@app.command()
def index(
    paths: t.List[str] = typer.Option(
        None, "-p", "--path", help="Source directory paths."
    ),
):
    cache = {}
    for path in paths or []:
        populate_source_cache(cache, lambda: get_directory_modules(path))
    typer.echo(cache)
    for lazy_source in cache.values():
        lazy_source()  # side effect of registering source
    for source in registry.get_sources():
        typer.echo(source)


if __name__ == "__main__":
    app()
