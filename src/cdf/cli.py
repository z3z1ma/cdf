"""CLI for cdf."""
import runpy
import tempfile
import typing as t
from enum import Enum
from pathlib import Path

import rich
import rich.traceback
import typer

import cdf.core.constants as c
from cdf.core.monads import Err, Ok, Result
from cdf.core.rewriter import intercepting_pipe_rewriter, rewrite_pipeline
from cdf.core.workspace import Project, augment_sys_path, load_project

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
) -> None:
    ctx.obj = load_project(root).unwrap()


@app.command(rich_help_panel="Project Info")
def index(ctx: typer.Context) -> None:
    """:page_with_curl: Print an index of [b][blue]Pipelines[/blue], [red]Models[/red], and [yellow]Publishers[/yellow][/b] loaded from the pipeline directory paths."""
    rich.print(ctx.obj)


@app.command(rich_help_panel="Project Info")
def docs(ctx: typer.Context) -> None:
    """:book: Render documentation for the project."""
    rich.print("Not implemented yet.")


@app.command(rich_help_panel="Project Info")
def path(
    ctx: typer.Context,
    workspace: str = typer.Argument(default=None),
) -> None:
    """:file_folder: Print the project root path. Pass a workspace to print the workspace root path."""
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
    """:mag: Evaluates a :zzz: Lazy [b blue]pipeline[/b blue] and enumerates the discovered resources."""
    project: Project = augment_sys_path(ctx.obj)
    fqn, err = Separator.split(pipeline, into=2).to_parts()
    if err:
        raise typer.BadParameter(
            f"Expected 2 parts <workspace>.<pipeline>. Got {pipeline}",
            param_hint="pipeline",
        )
    (workspace, pipeline) = fqn
    for source in _get_sources_or_raise(project, workspace, pipeline):
        rich.print(source.resources)


@app.command(rich_help_panel="Inspect")
def head(
    ctx: typer.Context,
    pipeline: t.Annotated[
        str, typer.Argument(help="The <workspace>.<pipeline>.<resource> to inspect.")
    ],
    num: t.Annotated[int, typer.Option("-n", "--num-rows")] = 5,
) -> None:
    """:wrench: Prints the first N rows of a [b green]Resource[/b green] within a [b blue]pipeline[/b blue]. Defaults to [cyan]5[/cyan].

    This is useful for quickly inspecting data :detective: and verifying that it is coming over the wire correctly.

    \f
    Args:
        ctx: The CLI context.
        pipeline: The pipeline to inspect.
        resource: The resource to inspect.
        num: The number of rows to print.

    Raises:
        typer.BadParameter: If the resource is not found in the pipeline.
    """
    project: Project = augment_sys_path(ctx.obj)
    fqn, err = Separator.split(pipeline, into=3).to_parts()
    if err:
        raise typer.BadParameter(
            f"Expected 3 parts <workspace>.<pipeline>.<resource>. Got {pipeline}",
            param_hint="pipeline",
        )
    (workspace, pipeline, resource) = fqn
    sources = _get_sources_or_raise(project, workspace, pipeline)
    candidates = [
        r for s in sources for r in s.resources.values() if r.name == resource
    ]
    if not candidates:
        raise typer.BadParameter(
            f"Resource {resource} not found in pipeline {pipeline}.",
            param_hint="resource",
        )
    r, i = iter(candidates[0]), 0
    while i < num:
        try:
            rich.print(next(r))
        except StopIteration:
            break
        i += 1


@app.command(rich_help_panel="Integrate")
def pipeline(
    ctx: typer.Context,
    pipeline: t.Annotated[
        str, typer.Argument(help="The <workspace>.<pipeline>.<sink> to run.")
    ],
    resources: t.List[str] = typer.Option(
        ..., "-r", "--resource", default_factory=list
    ),
    replace: t.Annotated[
        bool,
        typer.Option(
            ...,
            "-F",
            "--replace",
            help="Force the write disposition to replace ignoring state. Useful to force a full refresh of some resources.",
        ),
    ] = False,
) -> None:
    """:inbox_tray: Ingest data from a [b blue]pipeline[/b blue] into a data store where it can be [b red]Transformed[/b red].

    \f
    Args:
        ctx: The CLI context.
        opts: JSON formatted options to forward to the pipeline.
        resources: The resources to ingest.

    Raises:
        typer.BadParameter: If no resources are selected.
    """


@app.command(rich_help_panel="Integrate")
def publish(
    ctx: typer.Context,
    publisher: t.Annotated[
        str, typer.Argument(help="the <workspace>.<sink>.<publisher> to run")
    ],
    opts: str = typer.Argument(
        "{}", help="JSON formatted options to forward to the publisher."
    ),
    prompt_on_untracked: bool = typer.Option(
        True,
        help="Prompt the user before publishing untracked data. Defaults to True.",
    ),
) -> None:
    """:outbox_tray: [b yellow]Publish[/b yellow] data from a data store to an [violet]External[/violet] system."""


@app.command("execute-script", rich_help_panel="Utility")
def execute_script(
    ctx: typer.Context,
    script: t.Annotated[str, typer.Argument(help="The <workspace>.<script> to run")],
    opts: str = typer.Argument(
        "{}", help="JSON formatted options to forward to the script."
    ),
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
        opts: JSON formatted options to forward to the script.
    """


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


@app.command("develop", rich_help_panel="Project Initialization")
def develop(
    ctx: typer.Context,
    component: str = typer.Argument("*", help="The component to develop."),
) -> None:
    """:hammer_and_wrench: Install project components in the active virtual environment."""


def _get_sources_or_raise(project: Project, workspace: str, pipeline: str):
    """Get the sources from a dlt pipelines script or raise an error if unable to."""

    def _get_sources(code: str) -> t.Set[t.Any]:
        with tempfile.TemporaryDirectory() as tmpdir:
            f = Path(tmpdir) / "__main__.py"
            f.write_text(code)
            exports = runpy.run_path(
                tmpdir,
                run_name="__main__",
                init_globals={c.SOURCE_CONTAINER: set()},
            )
        return exports[c.SOURCE_CONTAINER]

    rich.print(f"Searching for {pipeline} in {workspace}...")
    sources, err = (
        project.search(workspace)
        .map(augment_sys_path)
        .bind(lambda w: w.search(pipeline, key="pipelines"))
        .map(lambda pipe: pipe.tree)
        .map(lambda tree: rewrite_pipeline(tree, rewriter=intercepting_pipe_rewriter))
        .map(lambda code: _get_sources(code.unwrap()))
        .to_parts()
    )
    if err:
        raise err
    return sources


class Separator(str, Enum):
    """
    Enum of delimiters for the CLI.

    We support the following syntaxes (with all combinations of delimiters)
    workspace.component
    workspace.component.sink
    workspace.component -> sink
    workspace.component >> sink
    workspace.component :: sink
    workspace.component | sink
    workspace >> component >> sink
    workspace/component/sink
    workspace/component-to-sink
    """

    DOT = "."
    DCOLON = "::"
    ARROW = "->"
    DARRROW = ">>"
    PIPE = "|"
    FSLASH = "/"
    TO = "-to-"

    @classmethod
    def split(cls, string: str, into: int = 2) -> Result[t.Tuple[str, ...], ValueError]:
        parts = [string]

        while delimiter := next((d.value for d in cls if d.value in parts[-1]), None):
            parts.extend(parts.pop().split(delimiter, 1))

        if len(parts) != into:
            return Err(ValueError(f"Expected {into} parts but got {len(parts)}."))
        return Ok(tuple(p.strip() for p in parts))


if __name__ == "__main__":
    app()
