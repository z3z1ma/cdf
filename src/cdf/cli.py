"""CLI for cdf."""

from pathlib import Path

import typer

import cdf.core.context as context
from cdf.core.configuration import load_config

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
    ctx.obj = load_config(path)
    context.active_workspace.set(workspace)
    context.inject_cdf_config_provider(ctx.obj.unwrap())


@app.command()
def init(ctx: typer.Context):
    """Initialize a new project."""
    typer.echo(ctx.obj)


@app.command()
def pipeline(ctx: typer.Context, source_to_dest: str):
    """Run a pipeline."""
    project = ctx.obj.unwrap()
    source, destination = source_to_dest.split(":", 1)
    source_config = project["pipelines"][source]
    typer.echo(source_config)
    breakpoint()
    typer.echo(source_config._root_path)
    typer.echo(
        f"Running pipeline from {source} to {destination} in {context.active_workspace.get()}."
    )
    import dlt
    import dlt.common.configuration

    @dlt.common.configuration.with_config(auto_pipeline_section=True)
    def foo(
        pipeline_name: str, x: int = dlt.config.value, y: int = dlt.config.value
    ) -> None:
        print((x, y))

    foo("us_cities")

    dlt.config.config_providers[-1].set_value("test123", 123, "us_cities")
    assert dlt.config["pipelines.us_cities.options.test123"] == 123

    dlt.config.config_providers[-1].set_value("test123", 123, "")
    assert dlt.config["test123"] == project["test123"]

    # set in pipeline options, which is very interesting
    pipeline = dlt.pipeline("us_cities")
    assert pipeline.runtime_config["dlthub_telemetry"] is False
    assert pipeline.destination.destination_type.endswith("duckdb")

    from cdf.core.feature_flag import load_feature_flag_provider
    from cdf.core.filesystem import load_filesystem_provider

    ff = load_feature_flag_provider("file", options={"path": "feature_flags.json"})
    fs = load_filesystem_provider()

    from cdf.core.specification.pipeline import PipelineSpecification, create_pipeline

    p = create_pipeline("us_cities", source_config)  # TODO: we need the root path?
    print(p)


if __name__ == "__main__":
    app()
