"""CLI for cdf."""

import typing as t
from pathlib import Path

import typer

import cdf.core.context as context
from cdf.core.project import get_project
from cdf.core.runtime import execute_pipeline_specification

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
def pipeline(ctx: typer.Context, source_to_dest: str) -> t.Any:
    """Run a pipeline."""
    workspace_name, path = ctx.obj
    workspace = (
        get_project(path).bind(lambda p: p.get_workspace(workspace_name)).unwrap()
    )
    context.inject_cdf_config_provider(workspace)
    token = context.active_project.set(workspace)
    try:
        source, destination = source_to_dest.split(":", 1)
        spec = workspace.get_pipeline(source).unwrap()
        exports = execute_pipeline_specification(spec, destination)
        typer.echo(spec.metric_state if spec.metric_state else "No metrics captured")
        return (
            exports.unwrap()
        )  # maybe a function which searches for LoadInfo objects from the exports
    finally:
        context.active_project.reset(token)


if __name__ == "__main__":
    app()
