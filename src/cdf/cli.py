"""CLI for cdf."""
import functools
import itertools
import typing as t
from enum import Enum
from pathlib import Path

import rich
import rich.traceback
import typer

import cdf.core.constants as c
import cdf.core.context as cdf_ctx
import cdf.core.logger as logger
from cdf.core.monads import Err, Ok, Result
from cdf.core.rewriter import (
    add_debugger,
    anchor_imports,
    apply_feature_flags,
    assert_recent_intervals,
    capture_sources,
    filter_resources,
    force_replace_disposition,
    noop,
    parametrize_destination,
    rewrite_script,
    set_basic_destination,
)
from cdf.core.sandbox import run
from cdf.core.workspace import Project, augment_sys_path, find_nearest

logger.monkeypatch_dlt()

app = typer.Typer(
    rich_markup_mode="rich",
    epilog="Made with [red]♥[/red] by [bold]z3z1ma[/bold].",
    add_completion=False,
    no_args_is_help=True,
)


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
    debug: bool = typer.Option(
        False, "--debug", "-d", help="Enable debug mode. Defaults to False."
    ),
) -> None:
    """CDF: Data Engineering Framework.

    \f
    Args:
        ctx: The CLI context.
        root: The project root path.
    """
    ctx.obj = find_nearest(root).unwrap()
    if debug:
        cdf_ctx.debug.set(True)


@app.command(rich_help_panel="Project Info")
def index(ctx: typer.Context) -> None:
    """:page_with_curl: Print an index of [b][blue]Pipelines[/blue], [red]Models[/red], and [yellow]Publishers[/yellow][/b] loaded from the pipeline directory paths.

    \f
    Args:
        ctx: The CLI context.
    """
    rich.print(ctx.obj)


@app.command(rich_help_panel="Project Info")
def docs(ctx: typer.Context) -> None:
    """:book: Render documentation for the project.

    \f
    Args:
        ctx: The CLI context.
    """
    rich.print("Not implemented yet.")


@app.command(rich_help_panel="Project Info")
def path(ctx: typer.Context, workspace: str = typer.Argument(default=None)) -> None:
    """:file_folder: Print the project root path. Pass a workspace to print the workspace root path.

    \f
    Args:
        ctx: The CLI context.
        workspace: The workspace to print the path for.
    """
    if workspace:
        rich.print(next(ws.root for ws in ctx.obj.members if ws.name == workspace))
    else:
        rich.print(ctx.obj.root)


@app.command(rich_help_panel="Inspect")
def discover(
    ctx: typer.Context,
    pipeline: t.Annotated[
        str, typer.Argument(help="The <workspace>.<pipeline> to discover.")
    ],
) -> None:
    """:mag: Evaluates a :zzz: Lazy [b blue]pipeline[/b blue] and enumerates the discovered resources.

    \f
    Args:
        ctx: The CLI context.
        pipeline: The pipeline to discover in the form of <workspace>.<pipeline>.
    """
    project: Project = augment_sys_path(ctx.obj)
    ws, pipe = Separator.split(pipeline, 2).unwrap()
    for i, source in enumerate(_get_sources_or_raise(project, ws, pipe), 1):
        rich.print(f"{i}: {source.name}")
        for j, resource in enumerate(source.resources.values(), 1):
            rich.print(f"{i}.{j}: {resource.name} (enabled: {resource.selected})")


@app.command(rich_help_panel="Inspect")
def head(
    ctx: typer.Context,
    resource: t.Annotated[
        str, typer.Argument(help="The <workspace>.<pipeline>.<resource> to inspect.")
    ],
    n: t.Annotated[int, typer.Option("-n", "--rows")] = 5,
) -> None:
    """:wrench: Prints the first N rows of a [b green]Resource[/b green] within a [b blue]pipeline[/b blue]. Defaults to [cyan]5[/cyan].

    This is useful for quickly inspecting data :detective: and verifying that it is coming over the wire correctly.

    \f
    Args:
        ctx: The CLI context.
        resource: The resource to inspect in the form of <workspace>.<pipeline>.<resource>.
        n: The number of rows to print.

    Raises:
        typer.BadParameter: If the resource is not found in the pipeline.
    """
    project: Project = augment_sys_path(ctx.obj)
    ws, pipe, resource = Separator.split(resource, 3).unwrap()
    target = next(
        filter(
            lambda r: r.name == resource,
            (
                resource
                for src in _get_sources_or_raise(project, ws, pipe)
                for resource in src.resources.values()
            ),
        ),
        None,
    )
    if target is None:
        raise typer.BadParameter(
            f"Resource {resource} not found in pipeline {pipe}.",
            param_hint="resource",
        )
    list(
        map(
            lambda it: rich.print(it[1]),
            itertools.takewhile(lambda it: it[0] < n, enumerate(target)),
        )
    )


@app.command(rich_help_panel="Integrate")
def pipeline(
    ctx: typer.Context,
    pipeline: t.Annotated[
        str, typer.Argument(help="The <workspace>.<pipeline> to run.")
    ],
    destination: t.Annotated[
        str, typer.Argument(help="The <workspace>.<sink> to run the pipeline to.")
    ],
    resources: t.List[str] = typer.Option(
        ...,
        "-r",
        "--resource",
        default_factory=lambda: [],
        help="Glob pattern for resources to run. Can be specified multiple times.",
    ),
    replace: t.Annotated[
        bool,
        typer.Option(
            ...,
            "-F",
            "--force-replace",
            help="Force the write disposition to replace ignoring state. Useful to force a reload of incremental resources.",
        ),
    ] = False,
) -> None:
    """:inbox_tray: Ingest data from a [b blue]pipeline[/b blue] into a data store where it can be [b red]Transformed[/b red].

    \f
    Args:
        ctx: The CLI context.
        pipeline: The pipeline to run in the form of <workspace>.<pipeline>.
        resources: The resources to ingest as a sequence of glob patterns.
        replace: Whether to force replace the write disposition.

    Raises:
        typer.BadParameter: If no resources are selected.
    """
    project: Project = augment_sys_path(ctx.obj)
    ws, pipe = Separator.split(pipeline, 2).unwrap()
    workspace = (
        project.search(ws)
        .map(functools.partial(augment_sys_path, parent=True))
        .unwrap()
    )
    with cdf_ctx.workspace_context(workspace):
        (
            Ok(workspace)
            .bind(lambda w: w.search(pipe, key=c.PIPELINES))
            .map(cdf_ctx.set_current_spec)
            .map(lambda pipe: pipe.tree)
            .bind(
                lambda tree: rewrite_script(
                    tree,
                    c.PIPELINES,
                    anchor_imports(c.PIPELINES),
                    (
                        Ok(workspace)  # Get the destination header
                        .bind(lambda w: w.search(destination, key=c.SINKS))
                        .map(lambda sink: sink.tree)
                        .unwrap_or(set_basic_destination(destination))
                    ),
                    parametrize_destination,
                    filter_resources(*resources) if resources else apply_feature_flags,
                    force_replace_disposition if replace else noop,
                    add_debugger if cdf_ctx.debug.get() else noop,
                )
            )
            .bind(lambda code: run(code, root=workspace.root))
            .unwrap()
        )


@app.command(rich_help_panel="Integrate")
def publish(
    ctx: typer.Context,
    publisher: t.Annotated[
        str, typer.Argument(help="the <workspace>.<publisher> to run")
    ],
) -> None:
    """:outbox_tray: [b yellow]Publish[/b yellow] data from a data store to an [violet]External[/violet] system."""
    project: Project = augment_sys_path(ctx.obj)
    ws, publisher = Separator.split(publisher, 2).unwrap()
    workspace = (
        project.search(ws)
        .map(functools.partial(augment_sys_path, parent=True))
        .unwrap()
    )
    with cdf_ctx.workspace_context(workspace):
        (
            Ok(workspace)
            .bind(lambda w: w.search(publisher, key=c.PUBLISHERS))
            .map(cdf_ctx.set_current_spec)
            .bind(
                lambda pipe: rewrite_script(
                    pipe.tree, c.PUBLISHERS, assert_recent_intervals
                )
            )
            .bind(lambda code: run(code, root=workspace.root))
            .unwrap()
        )


@app.command("execute-script", rich_help_panel="Utility")
def execute_script(
    ctx: typer.Context,
    script: t.Annotated[str, typer.Argument(help="The <workspace>.<script> to run")],
) -> None:
    """:rocket: Run a script in a workspace environment.

    A script is an arbitrary python file located in the ./scripts directory of a workspace. It defines an `entrypoint`
    function which takes a reference to the workspace as the first argument. Users can leverage cdf.with_config to
    inject configuration from the cdf_config file. Arbitrary keyword arguments can also be passed to the entrypoint
    function via the opts argument which is JSON formatted.

    \f
    Args:
        ctx: The CLI context.
        script: The script to run.
    """
    project: Project = augment_sys_path(ctx.obj)
    ws, script = Separator.split(script, 2).unwrap()
    workspace = (
        project.search(ws)
        .map(functools.partial(augment_sys_path, parent=True))
        .unwrap()
    )
    with cdf_ctx.workspace_context(workspace):
        (
            Ok(workspace)
            .bind(lambda w: w.search(script, key="scripts"))
            .map(cdf_ctx.set_current_spec)
            .bind(lambda pipe: rewrite_script(pipe.tree, c.SCRIPTS))
            .bind(lambda code: run(code, root=workspace.root))
            .unwrap()
        )


@app.command(rich_help_panel="Utility")
def jupyter(
    ctx: typer.Context,
    workspace: str = typer.Argument(
        default=None, help="The <workspace> to open jupyter lab in."
    ),
) -> None:
    """:rocket: Open juptyer lab in a workspace environment.

    \f
    Args:
        ctx: The CLI context.
    """
    rich.print("Not implemented yet.")


@app.command("execute-notebook", rich_help_panel="Utility")
def execute_notebook(
    ctx: typer.Context,
    notebook: t.Annotated[
        str, typer.Argument(help="The <workspace>.<notebook> to run")
    ],
    opts: str = typer.Argument(
        "{}", help="JSON formatted parameters to forward to the notebook."
    ),
) -> None:
    """:rocket: Run a notebook in a workspace environment.

    A notebook is an arbitrary ipynb file located in the ./notebooks directory of a workspace.

    \f
    Args:
        ctx: The CLI context.
        notebook: The notebook to run.
        opts: JSON formatted parameters to forward to the notebook.
    """
    rich.print("Not implemented yet.")


@app.command("generate-staging-layer", rich_help_panel="Utility")
def generate_staging_layer(
    ctx: typer.Context,
    sink: str,
    fetch_metadata_: bool = typer.Option(
        True,
        "-f",
        "--fetch-metadata/--no-fetch-metadata",
        help="Regenerate metadata before running",
    ),
    tables: t.List[str] = typer.Option(
        [],
        "-t",
        "--table",
        help="Glob pattern for tables to generate staging models for. Defaults to all. Can be specified multiple times.",
    ),
    overwrite: bool = typer.Option(
        False,
        "-o",
        "--overwrite",
        help="Overwrite existing staging models. Defaults to False.",
    ),
    sqlfmt_preset: bool = typer.Option(
        False,
        "-s",
        "--sqlfmt",
        help="A preset which will wrap the MODEL def with a no fmt directive and will format the model body with sqlfmt.",
    ),
) -> None:
    """:floppy_disk: Generate a staging layer for a catalog.

    After fetching metadata, this will generate a staging layer for each catalog. This is typically
    followed by cdf transform plan to materialize the staging layers.

    \f
    Args:
        ctx: The CLI context.
        sink: The sink to generate staging layers for.
        fetch_metadata: Whether to fetch metadata before generating staging layers.
    """
    rich.print("Not implemented yet.")


@app.command("init-workspace", rich_help_panel="Project Initialization")
def init_workspace(
    directory: t.Annotated[
        Path,
        typer.Argument(
            help="The directory to initialize the workspace in. Must be empty.",
            dir_okay=True,
            file_okay=False,
            resolve_path=True,
        ),
    ] = Path.cwd(),
) -> None:
    """:art: Initialize a new workspace.

    \f
    Args:
        directory: The directory to initialize the workspace in. Must be empty.
    """
    rich.print("Not implemented yet.")


@app.command("init-project", rich_help_panel="Project Initialization")
def init_project(
    ctx: typer.Context,
    directories: t.Annotated[
        t.List[Path],
        typer.Argument(
            help="The directory to initialize the project in. Must be empty.",
            dir_okay=True,
            file_okay=False,
            resolve_path=False,
        ),
    ],
    root: t.Annotated[
        Path,
        typer.Option(
            ..., "-r", "--root", help="The directory to initialize the project."
        ),
    ] = Path.cwd(),
) -> None:
    """:art: Initialize a new project in the current directory.

    \f
    Args:
        root: The directory to initialize the project in.
        directories: The directories in which to inialize workspaces relative to the project root.
    """
    rich.print("Not implemented yet.")


def _get_sources_or_raise(project: Project, ws: str, pipe: str):
    """Get the sources from a dlt pipelines script or raise an error if unable to."""
    from cdf.core.feature_flags import create_harness_provider

    ff_provider = create_harness_provider()
    workspace = project.search(ws).unwrap()
    with cdf_ctx.workspace_context(workspace):
        sources, err = (
            Ok(workspace)
            .map(functools.partial(augment_sys_path, parent=True))
            .bind(lambda w: w.search(pipe, key=c.PIPELINES))
            .map(cdf_ctx.set_current_spec)
            .map(lambda pipe: pipe.tree)
            .bind(
                lambda tree: rewrite_script(
                    tree,
                    c.PIPELINES,
                    anchor_imports(c.PIPELINES),
                    capture_sources,
                    add_debugger if cdf_ctx.debug.get() else noop,
                )
            )
            .bind(lambda code: run(code, root=workspace.root, quiet=True))
            .map(lambda exports: exports[c.SOURCE_CONTAINER])
            .to_parts()
        )
    if err:
        raise err
    return list(map(lambda s: ff_provider(s, workspace), sources))


class Separator(str, Enum):
    """
    Enum of delimiters for the CLI.

    We support the following syntaxes (with all combinations of delimiters)
    workspace.component
    workspace.component.sink
    workspace.component -> sink
    workspace.component >> sink
    workspace.component :: sink
    workspace >> component >> sink
    workspace/component/sink
    """

    DOT = "."
    DCOLON = "::"
    ARROW = "->"
    DARRROW = ">>"
    FSLASH = "/"

    @classmethod
    def split(
        cls, string: str, into: int = 2
    ) -> Result[t.Tuple[str, ...], typer.BadParameter]:
        parts = [string]

        while delimiter := next((d.value for d in cls if d.value in parts[-1]), None):
            parts.extend(parts.pop().split(delimiter, 1))

        if len(parts) != into:
            return Err(
                typer.BadParameter(
                    f"Expected {into} part fqn but parsed {len(parts)} from {string}."
                )
            )
        return Ok(tuple(p.strip() for p in parts))


if __name__ == "__main__":
    app()
