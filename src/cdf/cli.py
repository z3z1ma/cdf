"""CLI for cdf."""

from pathlib import Path

import typer

from cdf.core.project import load_project

app = typer.Typer(
    rich_markup_mode="rich",
    epilog="Made with [red]♥[/red] by [bold]z3z1ma[/bold].",
    add_completion=False,
    no_args_is_help=True,
)


@app.callback()
def main(ctx: typer.Context, path: Path = Path.cwd()):
    """CDF is a data pipeline framework."""
    ctx.obj = load_project(path)


@app.command()
def init(ctx: typer.Context):
    """Initialize a new project."""
    typer.echo(ctx.obj)


@app.command()
def pipeline(
    ctx: typer.Context,
    workspace: str,
    source: str = typer.Option(None, "--source", "-s"),
    destination: str = typer.Option(None, "--destination", "-d"),
):
    """Run a pipeline."""
    source_config = ctx.obj[(workspace, f"pipelines.{source}")]
    typer.echo(source_config)
    typer.echo(f"Running pipeline from {source} to {destination} in {workspace}.")


if __name__ == "__main__":
    app()
