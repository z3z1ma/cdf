"""CLI for cdf."""
import logging
import os
import subprocess
import typing as t
from contextlib import suppress
from pathlib import Path

import dlt
import dotenv
import rich
import typer

import cdf.core.constants as c
from cdf import CDFSourceWrapper, Project, logger
from cdf.core.utils import flatten_stream, fn_to_str

T = t.TypeVar("T")

app = typer.Typer(
    rich_markup_mode="rich",
    epilog="Made with [red]♥[/red] by [bold]z3z1ma[/bold].",
    add_completion=False,
)

dotenv.load_dotenv()


@app.callback()
def main(
    ctx: typer.Context,
    root: Path = typer.Option(
        ...,
        "-p",
        "--path",
        default_factory=Path.cwd,
        help="Path to the project root. Defaults to cwd. Parent dirs are searched for a workspace file.",
        envvar="CDF_ROOT",
    ),
    log_level: str = typer.Option(
        "INFO",
        "-l",
        "--log-level",
        help="Set the log level. Defaults to INFO.",
        envvar="CDF_LOG_LEVEL",
    ),
    debug: t.Annotated[
        bool,
        typer.Option(
            ..., "-d", "--debug", help="Run in debug mode, force log level to debug"
        ),
    ] = False,
):
    """:sparkles: a [b]framework[b] for managing and running [u]continousdataflow[/u] projects. :sparkles:

    [b/]
    - ( :electric_plug: ) [b blue]sources[/b blue]    are responsible for fetching data from a data source.
    - ( :shuffle_tracks_button: ) [b red]transforms[/b red] are responsible for transforming data in a data warehouse.
    - ( :mailbox: ) [b yellow]publishers[/b yellow] are responsible for publishing data to an external system.
    """
    logger.set_level(log_level.upper() if not debug else "DEBUG")
    ctx.obj = Project.find_nearest(root)
    ctx.obj.meta["root"] = root


def _inject_config_for_source(source: str, ctx: typer.Context) -> str:
    """Inject config into the CLI context.

    Args:
        source: The source name to inject config for.
        ctx: The CLI context.
    """
    project = ctx.obj
    workspace, _ = _parse_ws_component(source)
    if workspace not in project:
        raise typer.BadParameter(f"Workspace {workspace} not found.")
    project[workspace].inject_workspace_config_providers()
    return source


@app.command(rich_help_panel="Project Info")
def index(ctx: typer.Context) -> None:
    """:page_with_curl: Print an index of [b][blue]Sources[/blue], [red]Transforms[/red], and [yellow]Publishers[/yellow][/b] loaded from the source directory paths."""
    project: Project = ctx.obj
    rich.print(f" {project}")
    for _, workspace in project:
        rich.print(f"\n ~ {workspace}")
        if not any(workspace.capabilities.values()):
            rich.print(
                f"   No capabilities discovered. Add {c.SOURCES_PATH}, {c.TRANSFORMS_PATH}, or {c.PUBLISHERS_PATH}"
            )
            continue
        if workspace.has_sources:
            rich.print(f"\n   Sources Discovered: {len(workspace.sources)}")
            for i, (name, meta) in enumerate(workspace.sources.items(), start=1):
                fn = meta.factory.__wrapped__
                rich.print(f"   {i}) [b blue]{name}[/b blue] ({fn_to_str(fn)})")
        if workspace.has_transforms:
            rich.print("\n   Transforms Discovered: 0")
        if workspace.has_publishers:
            rich.print("\n   Publishers Discovered: 0")
        if workspace.has_dependencies:
            deps = workspace.requirements_path.read_text().splitlines()
            rich.print(f"\n   Dependencies: {len(deps)}")
            for i, dep in enumerate(deps):
                rich.print(f"   {i}) [b green]{dep}[/b green]")
    rich.print("")


@app.command(rich_help_panel="Project Info")
def docs(ctx: typer.Context) -> None:
    """:book: Render documentation for the project."""
    project: Project = ctx.obj
    docs_path = project.meta["root"].joinpath("docs")
    if not docs_path.exists():
        docs_path.mkdir()
    md_doc = "# CDF Project\n\n"
    for _, workspace in project:
        md_doc += f"## {workspace.namespace.title()} Space\n\n"
        if workspace.has_dependencies:
            md_doc += "### Dependencies\n\n"
            deps = subprocess.check_output([workspace.pip_path, "freeze"], text=True)
            for dep in deps.splitlines():
                md_doc += f"- `{dep}`\n"
            md_doc += "\n"
        if workspace.has_sources:
            md_doc += "### Sources\n\n"
            for name, meta in workspace.sources.items():
                md_doc += f"### {name}\n\n"
                md_doc += f"**Description**: {meta.description}\n\n"
                md_doc += f"**Owners**: {meta.owners}\n\n"
                md_doc += f"**Tags**: {', '.join(meta.tags)}\n\n"
                md_doc += f"**Cron**: {meta.cron or 'Not Scheduled'}\n\n"
                md_doc += f"**Metrics**: {meta.metrics}\n\n"
            md_doc += "\n"
        if workspace.has_transforms:
            md_doc += "### Transforms\n\n"
        if workspace.has_publishers:
            md_doc += "### Publishers\n\n"
    rich.print(md_doc)


@app.command(rich_help_panel="Inspect")
def discover(
    ctx: typer.Context,
    source: t.Annotated[str, typer.Argument(callback=_inject_config_for_source)],
) -> None:
    """:mag: Evaluates a :zzz: Lazy [b blue]Source[/b blue] and enumerates the discovered resources."""
    logger.debug("Discovering source %s", source)
    project: Project = ctx.obj
    ws, src = _parse_ws_component(source)
    with project[ws].get_runtime_source(src) as rt_source:
        rich.print(
            f"\nDiscovered {len(rt_source.resources)} resources in"
            f" [b red]{source}.v{project[ws][src].version}[/b red]:"
        )
        for i, resource in enumerate(rt_source.resources.values(), start=1):
            if resource.selected:
                rich.print(f"  {i}) [b green]{resource.name}[/b green] (enabled: True)")
            else:
                rich.print(f"  {i}) [b red]{resource.name}[/b red] (enabled: False)")
        _print_meta(project[ws][src])


@app.command(rich_help_panel="Inspect")
def head(
    ctx: typer.Context,
    source: t.Annotated[str, typer.Argument(callback=_inject_config_for_source)],
    resource: str,
    num: t.Annotated[int, typer.Option("-n", "--num-rows")] = 5,
) -> None:
    """:wrench: Prints the first N rows of a [b green]Resource[/b green] within a [b blue]Source[/b blue]. Defaults to [cyan]5[/cyan].

    This is useful for quickly inspecting data :detective: and verifying that it is coming over the wire correctly.
    """
    project: Project = ctx.obj
    ws, src = _parse_ws_component(source)
    with project[ws].get_runtime_source(src) as rt_source:
        if resource not in rt_source.resources:
            raise typer.BadParameter(
                f"Resource {resource} not found in source {source}."
            )
        res = rt_source.resources[resource]
        rich.print(
            f"\nHead of [b red]{resource}[/b red] in [b blue]{source}.v{project[ws][src].version}[/b blue]:"
        )
        it = flatten_stream(res)
        while num > 0 and (v := next(it, None)):
            rich.print(v)
            v = next(it, None)
            num -= 1


@app.command(rich_help_panel="Integrate")
def ingest(
    ctx: typer.Context,
    source: t.Annotated[str, typer.Argument(callback=_inject_config_for_source)],
    dest: t.Annotated[str, typer.Option(..., "-d", "--dest")] = "default",
    resources: t.List[str] = typer.Option(
        ..., "-r", "--resource", default_factory=list
    ),
) -> None:
    """:inbox_tray: Ingest data from a [b blue]Source[/b blue] into a data store where it can be [b red]Transformed[/b red].

    \f
    Args:
        ctx: The CLI context.
        source: The source to ingest from.
        dest: The destination to ingest to.
        resources: The resources to ingest.

    Raises:
        typer.BadParameter: If no resources are selected.
    """
    project: Project = ctx.obj
    ws, src = _parse_ws_component(source)
    with project[ws].get_runtime_source(src) as rt_source:
        if resources:
            rt_source = rt_source.with_resources(*resources)
        if not rt_source.selected_resources:
            raise typer.BadParameter(
                f"No resources selected for source {source}. Use the discover command to see available resources.\n"
                "Select them explicitly with --resource or enable them with feature flags.\n\n"
                f"Reach out to the source owners for more information: {project[ws][src].owners}"
            )
        dataset_name = f"{src}_v{project[ws][src].version}"
        rich.print(
            f"Ingesting data from [b blue]{source}[/b blue] to [b red]{dest}[/b red]..."
        )
        for resource in rt_source.selected_resources:
            rich.print(f"  - [b green]{resource}[/b green]")
        engine, dest = dest.split(".", 1)
        pkwargs = {}
        if "BUCKET_URL" in os.environ:
            # Staging native creds use expected cloud provider env vars
            # such as GOOGLE_APPLICATION_CREDENTIALS, AWS_ACCESS_KEY_ID, etc.
            pkwargs["staging"] = "filesystem"
        with suppress(KeyError):
            # Permit credentials to be omitted which will fall back to native parser
            pkwargs["credentials"] = dlt.secrets[f"{engine}.{dest}.credentials"]
        pipeline = dlt.pipeline(
            f"cdf-{src}",
            destination=engine,
            dataset_name=dataset_name,
            progress="alive_progress",
            **pkwargs,
        )
        info = pipeline.run(rt_source)
    logging.info(info)


@app.command(rich_help_panel="Integrate")
def transform() -> None:
    """:arrows_counterclockwise: [b red]Transform[/b red] data from a data store into a data store where it can be exposed or [b yellow]Published[/b yellow]."""
    rich.print("Transforming with SQLMesh...")


@app.command(rich_help_panel="Integrate")
def publish() -> None:
    """:outbox_tray: [b yellow]Publish[/b yellow] data from a data store to an [violet]External[/violet] system."""
    rich.print("Publishing...")


@app.command(rich_help_panel="Utility")
def run(
    ctx: typer.Context,
    args: t.List[str] = typer.Argument(allow_dash=True),
) -> None:
    """:rocket: Run an executable located in a workspace environment.

    This is useful for running packages installed in a workspace environment without having to activate it first.
    It is purely a convenience method and is not required to operate within a CDF project. All arguments should
    be passed after a -- separator.

    \f
    Example:
        cdf run my_workspace.pip -- --help
        cdf run my_workspace.gcloud -- --help

    Args:
        ctx: The CLI context.
        args: The executable followed by arguments to forward to it.

    Raises:
        subprocess.CalledProcessError: If the executable returns a non-zero exit code.
    """
    project: Project = ctx.obj
    executable = args.pop(0)
    ws, component = _parse_ws_component(executable)
    rich.print(">>> Running", ws, component)
    with project[ws].environment():
        subprocess.check_call([project[ws].get_bin(component), *args])


def _parse_ws_component(component: str) -> t.Tuple[str, str]:
    """Parse a workspace.component string into a tuple.

    Args:
        component: The component string to parse.

    Returns:
        A tuple of (workspace, component).
    """
    if "." in component:
        ws, src = component.split(".", 1)
        return ws, src
    return c.DEFAULT_WORKSPACE, component


def _print_meta(meta: CDFSourceWrapper) -> None:
    rich.print(f"\nOwners: [yellow]{meta.owners}[/yellow]")
    rich.print(f"Description: {meta.description}")
    rich.print(f"Tags: {meta.tags}")
    rich.print(f"Cron: {meta.cron}\n")


if __name__ == "__main__":
    app()
