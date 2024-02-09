"""CLI for cdf."""
import tempfile
import typing as t
from enum import Enum
from pathlib import Path

import rich
import rich.traceback
import typer
from croniter import croniter

from cdf.core.monads import Err, Ok, Result
from cdf.core.pipeline import CDFReturn
from cdf.core.workspace import Project, load_project

app = typer.Typer(
    rich_markup_mode="rich",
    epilog="Made with [red]♥[/red] by [bold]z3z1ma[/bold].",
    add_completion=False,
    no_args_is_help=True,
)


class Delimiter(str, Enum):
    """Enum of delimiters for the CLI."""

    DOT = "."
    DCOLON = "::"
    ARROW = "->"
    DARRROW = ">>"
    PIPE = "|"
    FSLASH = "/"
    TO = "-to-"

    @classmethod
    def _split(
        cls, string: str, into: int = 2
    ) -> Result[t.Tuple[str, ...], ValueError]:
        parts = [string]

        while delimiter := next((d.value for d in cls if d.value in parts[-1]), None):
            parts.extend(parts.pop().split(delimiter, 1))

        if len(parts) != into:
            return Err(ValueError(f"Expected {into} parts but got {len(parts)}."))
        return Ok(tuple(p.strip() for p in parts))


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
    opts: str = typer.Argument(
        "{}", help="JSON formatted options to forward to the pipeline."
    ),
) -> None:
    """:mag: Evaluates a :zzz: Lazy [b blue]pipeline[/b blue] and enumerates the discovered resources."""
    project: Project = ctx.obj
    if len(project.members) > 1:
        segments, err = Delimiter._split(pipeline, into=2).to_parts()
        if err:
            raise typer.BadParameter("Invalid pipeline format.") from err
        workspace, pipeline = segments
    else:
        workspace, pipeline = project.name, pipeline
    rich.print(f"Searching for {pipeline} in {workspace}...")
    maybe_pipe = project.search(workspace).bind(
        lambda ws: ws.search(pipeline, key="pipelines")
    )
    if maybe_pipe.is_nothing():
        raise typer.BadParameter(
            f"Pipeline {pipeline} not found in {workspace}. Ensure the workspace exists and the pipeline is defined."
        )
    pipe = maybe_pipe.unwrap()
    import os
    import runpy

    os.environ["RUNTIME__LOG_LEVEL"] = "INFO"
    with tempfile.TemporaryDirectory() as tmpdir:
        f = Path(tmpdir) / "__main__.py"
        # TODO: rewriting should be deferred since we do different things based on the entrypoint
        f.write_bytes(pipe.runtime_code.encode("utf-8"))
        try:
            # TODO: set __module__ (or whatever it is) to enable relative imports form workspace
            runpy.run_path(str(f.absolute()), run_name="__main__")
        except CDFReturn as e:
            sources = e.value
            for source in sources:
                rich.print(source)
    rich.print(pipe)


@app.command(rich_help_panel="Inspect")
def head(
    ctx: typer.Context,
    pipeline: t.Annotated[
        str, typer.Argument(help="The <workspace>.<pipeline>.<resource> to inspect.")
    ],
    opts: str = typer.Argument(
        "{}", help="JSON formatted options to forward to the pipeline."
    ),
    num: t.Annotated[int, typer.Option("-n", "--num-rows")] = 5,
) -> None:
    """:wrench: Prints the first N rows of a [b green]Resource[/b green] within a [b blue]pipeline[/b blue]. Defaults to [cyan]5[/cyan].

    This is useful for quickly inspecting data :detective: and verifying that it is coming over the wire correctly.

    \f
    Args:
        ctx: The CLI context.
        pipeline: The pipeline to inspect.
        resource: The resource to inspect.
        opts: JSON formatted options to forward to the pipeline.
        num: The number of rows to print.

    Raises:
        typer.BadParameter: If the resource is not found in the pipeline.
    """


@app.command(rich_help_panel="Integrate")
def pipeline(
    ctx: typer.Context,
    pipeline: t.Annotated[
        str, typer.Argument(help="The <workspace>.<pipeline>.<sink> to run.")
    ],
    opts: str = typer.Argument(
        "{}", help="JSON formatted options to forward to the pipeline."
    ),
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
        pipeline: The pipeline to ingest from and the sink to ingest into.
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


def _parse_ws_component(
    component: str, project: "Project | None" = None
) -> t.Tuple[str, ...]:
    """Parse a workspace.component string into a tuple of parts.

    We support the following syntaxes (with all combinations of delimiters)
    workspace.component
    workspace.component.sink
    workspace.component -> sink
    workspace.component >> sink
    workspace.component :: sink
    workspace.component | sink
    workspace >> component >> sink
    workspace/component/sink

    if operating in a project with a default workspace indicating a flat single-tenant structure,
    no workspace should be specified in the component string. Same goes for a single workspace project.

    Args:
        component: The component string to parse.

    Returns:
        A tuple of parts.
    """
    parts = [component]

    # Parse
    while delim := next(
        (d for d in Delimiter if d.value in parts[-1]),
        None,
    ):
        parts.extend(parts.pop(-1).split(delim.value, 1))

    parts = [p.strip() for p in parts]

    # Inject workspace in a single-tenant project
    if project and len(project) == 1:
        ws = next(iter(project))
        if parts[0] != ws:
            parts.insert(0, ws)
    if project and parts[0] not in project:
        raise ValueError(f"Workspace {parts[0]} not found in project.")

    return tuple(parts)


def _print_metadata(metadata) -> None:
    """
    Print common component metadata.

    Args:
        meta: The component metadata.
    """
    rich.print(f"\n[b]Owners[/b]: [yellow]{metadata.owner}[/yellow]")
    description = metadata.description.replace("\n", " ")
    rich.print(f"[b]Description[/b]: {description}")
    rich.print(f"[b]Tags[/b]: {', '.join(metadata.tags)}")
    if metadata.cron:
        cron = (
            " ".join(metadata.cron.expressions)
            if isinstance(metadata.cron, croniter)
            else metadata.cron
        )
        rich.print(f"[b]Cron[/b]: {cron}\n")


def _metadata_to_md_section(name: str, metadata) -> str:
    """Convert a component's metadata to a markdown section."""
    md_doc = f"#### {name}\n\n"
    md_doc += f"- **Description**: {metadata.description}\n"
    md_doc += f"- **Owners**: {metadata.owner}\n"
    md_doc += f"- **Tags**: {', '.join(metadata.tags)}\n"
    md_doc += f"- **Cron**: {metadata.cron or 'Not Scheduled'}\n\n"
    return md_doc


if __name__ == "__main__":
    app()
