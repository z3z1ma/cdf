"""CLI for cdf."""
import typing as t

import typer

from cdf import get_directory_modules, load_sources, registry

app = typer.Typer()


@app.command()
def index(
    paths: t.List[str] = typer.Option(
        None, "-p", "--path", help="Source directory paths."
    ),
):
    cache = {}
    for path in paths or []:
        load_sources(get_directory_modules(path), cache=cache, lazy_sources=False)
    typer.echo(cache)
    for lazy_source in cache.values():
        lazy_source()  # side effect of registering source
    for source in registry.get_sources():
        typer.echo(source)


if __name__ == "__main__":
    app()
