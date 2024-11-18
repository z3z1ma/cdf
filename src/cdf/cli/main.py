# pyright: reportUnknownVariableType=false
import sys
from pathlib import Path

import click

from cdf.core import Project


@click.group()
@click.option("--project-path", default=".", help="Path to the project directory.")
@click.pass_context
def cli(ctx: click.Context, project_path: Path | str) -> None:
    """CLI for managing CDF projects and data packages."""
    _ = ctx.ensure_object(dict)
    project_path = Path(project_path)
    if not project_path.exists():
        click.echo(f"Project path {project_path} does not exist.", err=True)
        sys.exit(1)
    ctx.obj["PROJECT"] = Project(project_path)


@cli.command()
@click.pass_context
def list_packages(ctx: click.Context) -> None:
    """List all data packages in the project."""
    project = ctx.obj["PROJECT"]
    packages = project.data_packages.keys()
    if not packages:
        click.echo("No data packages found.")
    else:
        click.echo("Data Packages:")
        for pkg in packages:
            click.echo(f"- {pkg}")


@cli.command()
@click.argument("data_package")
@click.pass_context
def list_schedules(ctx: click.Context, data_package: str) -> None:
    """List schedules for a data package."""
    project = ctx.obj["PROJECT"]
    if data_package in project.data_packages:
        pkg = project.data_packages[data_package]
        schedules = pkg.schedules
        if not schedules:
            click.echo(f"No schedules found for data package '{data_package}'.")
        else:
            click.echo(f"Schedules for data package '{data_package}':")
            for schedule in schedules:
                click.echo(f"- {schedule}")
    else:
        click.echo(f"Data package '{data_package}' not found.", err=True)
        sys.exit(1)


@cli.command()
@click.argument("data_package")
@click.pass_context
def discover_pipelines(ctx: click.Context, data_package: str) -> None:
    """Discover pipelines in a data package."""
    project = ctx.obj["PROJECT"]
    if data_package in project.data_packages:
        pkg = project.data_packages[data_package]
        pipelines = pkg.discover_extract_load_pipelines()
        if not pipelines:
            click.echo(f"No pipelines found in data package '{data_package}'.")
        else:
            click.echo(f"Pipelines in data package {data_package}:")
            for name in pipelines:
                click.echo(f"- {name}")
    else:
        click.echo(f"Data package '{data_package}' not found.", err=True)
        sys.exit(1)


@cli.command()
@click.argument("data_package")
@click.argument("pipeline_name")
@click.option(
    "--kwargs",
    "-k",
    multiple=True,
    help="Additional arguments for the pipeline in key=value format.",
)
@click.pass_context
def run_pipeline(
    ctx: click.Context, data_package: str, pipeline_name: str, kwargs: list[str]
) -> None:
    """Run a specific pipeline in a data package."""
    project = ctx.obj["PROJECT"]
    if data_package in project.data_packages:
        pkg = project.data_packages[data_package]
        kwargs_dict = {}
        for item in kwargs:
            if "=" in item:
                key, value = item.split("=", 1)
                kwargs_dict[key] = value
            else:
                click.echo(f"Invalid argument format: {item}. Use key=value.", err=True)
                sys.exit(1)
        try:
            pkg.run_pipeline(pipeline_name, **kwargs_dict)
            click.echo(
                f"Pipeline '{pipeline_name}' in data package '{data_package}' executed successfully."
            )
        except Exception as e:
            click.echo(f"Error running pipeline: {e}", err=True)
            sys.exit(1)
    else:
        click.echo(f"Data package '{data_package}' not found.", err=True)
        sys.exit(1)


@cli.command()
@click.argument("data_package")
@click.pass_context
def run_tests(ctx: click.Context, data_package: str) -> None:
    """Run tests for a data package."""
    project = ctx.obj["PROJECT"]
    if data_package in project.data_packages:
        pkg = project.data_packages[data_package]
        try:
            results = pkg.run_tests()
            click.echo(f"Test results for data package '{data_package}':")
            click.echo(results)
        except AssertionError as e:
            click.echo(str(e), err=True)
            sys.exit(1)
        except Exception as e:
            click.echo(f"Error running tests: {e}", err=True)
            sys.exit(1)
    else:
        click.echo(f"Data package '{data_package}' not found.", err=True)
        sys.exit(1)


@cli.command()
@click.pass_context
def show_config(ctx: click.Context) -> None:
    """Show the project configuration."""
    project = ctx.obj["PROJECT"]
    click.echo("Project Configuration:")
    for key, value in project.config.items():
        click.echo(f"{key}: {value}")


@cli.command()
@click.argument("data_package")
@click.pass_context
def show_package_config(ctx: click.Context, data_package: str) -> None:
    """Show the configuration for a data package."""
    project = ctx.obj["PROJECT"]
    if data_package in project.data_packages:
        pkg = project.data_packages[data_package]
        click.echo(f"Configuration for data package '{data_package}':")
        for key, value in pkg.config.items():
            click.echo(f"{key}: {value}")
    else:
        click.echo(f"Data package '{data_package}' not found.", err=True)
        sys.exit(1)


if __name__ == "__main__":
    cli()
