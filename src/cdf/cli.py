"""CLI for cdf."""
import typing as t

import typer

from cdf import DirectoryLoader, registry

app = typer.Typer()


@app.command()
def index(
    paths: t.List[str] = typer.Option(
        None, "-p", "--path", help="Source directory paths."
    ),
):
    cache = {}
    for path in paths or []:
        DirectoryLoader(path, cache=cache, load=True)
    typer.echo(cache)
    for lazy_source in cache.values():
        lazy_source()  # side effect of registering source
    for source in registry.get_sources():
        typer.echo(source)


if __name__ == "__main__":
    app()
