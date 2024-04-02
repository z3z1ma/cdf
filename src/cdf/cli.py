"""CLI for cdf."""

import typing as t
from pathlib import Path

import typer

import cdf.core.context as context
from cdf.core.project import Workspace, get_project
from cdf.types import M

app = typer.Typer(
    rich_markup_mode="rich",
    epilog="Made with [red]♥[/red] by [bold]z3z1ma[/bold].",
    add_completion=False,
    no_args_is_help=True,
)


@app.callback()
def main(
    ctx: typer.Context,
    workspace: str,
    path: Path = typer.Option(".", "--path", "-p", help="Path to the project."),
):
    """CDF is a data pipeline framework."""
    ctx.obj = workspace, path


@app.command()
def init(ctx: typer.Context):
    """Initialize a new project."""
    typer.echo(ctx.obj)


@app.command()
def pipeline(ctx: typer.Context, source_to_dest: str):
    """Run a pipeline."""
    workspace, path = ctx.obj
    workspace = (
        get_project(path).bind(lambda p: p.get_workspace(str(workspace))).unwrap()
    )
    context.inject_cdf_config_provider(workspace)
    token = context.active_project.set(workspace)
    try:
        source, destination = source_to_dest.split(":", 1)
        workspace.get_pipeline(source).map(lambda p: p(destination)).unwrap()
    finally:
        context.active_project.reset(token)


if __name__ == "__main__":
    app()
