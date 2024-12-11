# pyright: reportUnknownVariableType=false
"""CLI for managing CDF projects and data packages."""

import os
import sys
from pathlib import Path

import click

import cdf

_PROJECT_CONTEXT = "PROJECT"
"""Name of the variable in click context to store the project."""

_ACTIVE_PACKAGE_CONTEXT = "CDF_ACTIVE_PACKAGE"
"""An env variable to store the active data package indicating the deps are resolved."""


@click.group()
@click.option("--project-path", "-p", default=".", help="Path to the project directory.")
@click.pass_context
def cli(ctx: click.Context, project_path: Path | str) -> None:
    """CLI for managing CDF projects and data packages."""
    _ = ctx.ensure_object(dict)
    project_path = Path(project_path)
    if not project_path.exists():
        click.echo(f"Project path '{project_path}' does not exist.", err=True)
        sys.exit(1)
    ctx.obj[_PROJECT_CONTEXT] = cdf.Project(project_path)


def _get_project(ctx: click.Context) -> cdf.Project:
    """Helper function to get the project from context."""
    return ctx.obj.get(_PROJECT_CONTEXT)


def _get_package(ctx: click.Context, data_package: str) -> cdf.DataPackage:
    """Helper function to get a data package ensuring it is active or exit."""
    project = _get_project(ctx)
    if data_package not in project:
        click.echo(f"Data package '{data_package}' not found.", err=True)
        sys.exit(1)
    # HACK: an interesting way to ensure dependencies are installed just-in-time for
    # a given package without fussing with venvs directly
    if os.getenv(_ACTIVE_PACKAGE_CONTEXT) != data_package:
        os.execvpe(
            "uv",
            args=["uv", "run", "--package", data_package, *sys.argv],
            env={**os.environ, _ACTIVE_PACKAGE_CONTEXT: data_package},
        )
    return project[data_package]


@cli.command()
@click.pass_context
def list_packages(ctx: click.Context) -> None:
    """List all data packages in the project."""
    project = _get_project(ctx)
    packages = list(project.data_packages.keys())
    if not packages:
        click.echo("No data packages found.")
    else:
        click.echo("Data Packages:")
        for pkg_name in packages:
            click.echo(f"- {pkg_name}")


@cli.command()
@click.argument("data_package")
@click.pass_context
def list_schedules(ctx: click.Context, data_package: str) -> None:
    """List schedules for a data package."""
    pkg = _get_package(ctx, data_package)
    schedules = pkg.settings.schedules
    if not schedules:
        click.echo(f"No schedules found for data package '{data_package}'.")
    else:
        click.echo(f"Schedules for data package '{data_package}':")
        for schedule in schedules:
            click.echo(f"- {schedule}")


@cli.command()
@click.argument("data_package")
@click.pass_context
def discover_pipelines(ctx: click.Context, data_package: str) -> None:
    """Discover pipelines in a data package."""
    pkg = _get_package(ctx, data_package)
    try:
        pipelines = pkg.discover_ingest_pipelines()
        if not pipelines:
            click.echo(f"No pipelines found in data package '{data_package}'.")
        else:
            click.echo(f"Pipelines in data package '{data_package}':")
            for name in pipelines:
                click.echo(f"- {name}")
    except Exception as e:
        click.echo(f"Error discovering pipelines: {e}", err=True)
        sys.exit(1)


@cli.command()
@click.argument("data_package")
@click.argument("pipeline_name", default="main")
@click.option(
    "--kwargs",
    "-k",
    multiple=True,
    help="Additional arguments for the pipeline in key=value format.",
)
@click.pass_context
def ingest(
    ctx: click.Context, data_package: str, pipeline_name: str = "main", *, kwargs: list[str]
) -> None:
    """Run a specific pipeline in a data package."""
    pkg = _get_package(ctx, data_package)
    kwargs_dict = {}
    for item in kwargs:
        if "=" in item:
            key, value = item.split("=", 1)
            kwargs_dict[key] = cdf.config.apply_converters(value, pkg.container.cfg)
        else:
            click.echo(f"Invalid argument format: '{item}'. Use key=value.", err=True)
            sys.exit(1)
    try:
        pkg.ingest(pipeline_name, **kwargs_dict)
        click.echo(
            f"Pipeline '{pipeline_name}' in data package '{data_package}' executed successfully."
        )
    except Exception as e:
        click.echo(f"Error running pipeline: {e}", err=True)
        sys.exit(1)


@cli.command()
@click.argument("data_package")
@click.pass_context
def test(ctx: click.Context, data_package: str) -> None:
    """Run tests for a data package."""
    pkg = _get_package(ctx, data_package)
    try:
        results = pkg.test()
        click.echo(f"Test results for data package '{data_package}':")
        click.echo(results)
    except AssertionError as e:
        click.echo(str(e), err=True)
        sys.exit(1)
    except Exception as e:
        click.echo(f"Error running tests: {e}", err=True)
        sys.exit(1)


@cli.command(
    context_settings=dict(
        ignore_unknown_options=True,
        allow_extra_args=True,
    ),
)
@click.argument("data_package")
@click.pass_context
def transform(ctx: click.Context, data_package: str) -> None:
    """Run transforms for a data package."""
    pkg = _get_package(ctx, data_package)
    try:
        pkg.transform(*ctx.args)
        click.echo(f"Successfully ran transforms for data package '{data_package}'.")
    except Exception as e:
        click.echo(f"Error running transforms: {e}", err=True)
        sys.exit(1)


@cli.command()
@click.pass_context
def dump_config(ctx: click.Context) -> None:
    """Show the project configuration."""
    project = _get_project(ctx)
    click.echo("Project Configuration:")
    for key, value in project.settings.model_dump().items():
        click.echo(f"{key}: {value}")


@cli.command()
@click.argument("data_package")
@click.pass_context
def dump_package_config(ctx: click.Context, data_package: str) -> None:
    """Show the configuration for a data package."""
    pkg = _get_package(ctx, data_package)
    click.echo(f"Configuration for data package '{data_package}':")
    for key, value in pkg.settings.model_dump().items():
        click.echo(f"{key}: {value}")


if __name__ == "__main__":
    cli()
