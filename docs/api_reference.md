<a id="__init__"></a>

# \_\_init\_\_

<a id="__init__.find_nearest"></a>

#### find\_nearest

```python
@M.result
def find_nearest(path: PathLike = ".") -> Project
```

Find the nearest project.

Recursively searches for a project file in the parent directories.

**Arguments**:

- `path` _PathLike, optional_ - The path to start searching from. Defaults to ".".
  

**Raises**:

- `FileNotFoundError` - If no project is found.
  

**Returns**:

- `Project` - The nearest project.

<a id="__init__.is_main"></a>

#### is\_main

```python
def is_main(module_name: t.Optional[str] = None) -> bool
```

Check if the current module is being run as the main program in cdf context.

Also injects a hook in debug mode to allow dropping into user code via pdb.

**Arguments**:

- `module_name` _str, optional_ - The name of the module to check. If None, the calling module is
  checked. The most idiomatic usage is to pass `__name__` to check the current module.
  

**Returns**:

- `bool` - True if the current module is the main program in cdf context.

<a id="__init__.get_active_project"></a>

#### get\_active\_project

```python
def get_active_project() -> Project
```

Get the active project.

**Raises**:

- `ValueError` - If no valid project is found in the context.
  

**Returns**:

- `Project` - The active project.

<a id="__init__.get_workspace"></a>

#### get\_workspace

```python
def get_workspace(path: PathLike = ".") -> M.Result[Workspace, Exception]
```

Get a workspace from a path.

**Arguments**:

- `path` _PathLike, optional_ - The path to get the workspace from. Defaults to ".".
  

**Returns**:

  M.Result[Workspace, Exception]: The workspace or an error.

<a id="__init__.transform_gateway"></a>

#### transform\_gateway

Gateway configuration for transforms.

<a id="__init__.transform_connection"></a>

#### transform\_connection

```python
def transform_connection(type_: str, **kwargs) -> ConnectionConfig
```

Create a connection configuration for transforms.

<a id="__init__.run_script"></a>

#### run\_script

```python
def run_script(module_name: str,
               source: t.Union[t.Callable[..., dlt.sources.DltSource],
                               dlt.sources.DltSource],
               *,
               run_options: t.Optional[t.Dict[str, t.Any]] = None,
               **kwargs: t.Any) -> None
```

A shorthand syntax for a cdf script with a single source which should run as a pipeline.

The first argument should almost always be `__name__`. This function conditionally executes
the source if the module is the main program in cdf context. This occurs either when invoked
through cdf <workspace> pipeline command or when the script is run directly by python.

<a id="cli"></a>

# cli

CLI for cdf.

<a id="cli.main"></a>

#### main

```python
@app.callback()
def main(
    ctx: typer.Context,
    workspace: str,
    path: Path = typer.Option(".",
                              "--path",
                              "-p",
                              help="Path to the project.",
                              envvar="CDF_ROOT"),
    debug: bool = typer.Option(False,
                               "--debug",
                               "-d",
                               help="Enable debug mode."),
    environment: t.Optional[str] = typer.Option(None,
                                                "--env",
                                                "-e",
                                                help="Environment to use."),
    log_level: t.Optional[str] = typer.Option(
        None,
        "--log-level",
        "-l",
        help="The log level to use.",
        envvar="LOG_LEVEL",  # A common environment variable for log level
    )
) -> None
```

CDF (continuous data framework) is a framework for end to end data processing.

<a id="cli.init"></a>

#### init

```python
@app.command(rich_help_panel="Project Management")
def init(ctx: typer.Context) -> None
```

:art: Initialize a new project.

<a id="cli.index"></a>

#### index

```python
@app.command(rich_help_panel="Project Management")
def index(ctx: typer.Context, hydrate: bool = False) -> None
```

:page_with_curl: Print an index of [b][blue]Pipelines[/blue], [red]Models[/red], [yellow]Publishers[/yellow][/b], and other components.

<a id="cli.pipeline"></a>

#### pipeline

```python
@app.command(rich_help_panel="Core")
def pipeline(
    ctx: typer.Context,
    pipeline_to_sink: t.Annotated[
        str,
        typer.Argument(help="The pipeline and sink separated by a colon."),
    ],
    select: t.List[str] = typer.Option(
        ...,
        "-s",
        "--select",
        default_factory=lambda: [],
        help=
        "Glob pattern for resources to run. Can be specified multiple times.",
    ),
    exclude: t.List[str] = typer.Option(
        ...,
        "-x",
        "--exclude",
        default_factory=lambda: [],
        help=
        "Glob pattern for resources to exclude. Can be specified multiple times.",
    ),
    force_replace: t.Annotated[
        bool,
        typer.Option(
            ...,
            "-F",
            "--force-replace",
            help=
            "Force the write disposition to replace ignoring state. Useful to force a reload of incremental resources.",
        ),
    ] = False,
    no_stage: t.Annotated[
        bool,
        typer.Option(
            ...,
            "--no-stage",
            help=
            "Do not stage the data in the staging destination of the sink even if defined.",
        ),
    ] = False
) -> t.Any
```

:inbox_tray: Ingest data from a [b blue]Pipeline[/b blue] into a data store where it can be [b red]Transformed[/b red].


**Arguments**:

- `ctx` - The CLI context.
- `pipeline_to_sink` - The pipeline and sink separated by a colon.
- `select` - The resources to ingest as a sequence of glob patterns.
- `exclude` - The resources to exclude as a sequence of glob patterns.
- `force_replace` - Whether to force replace the write disposition.
- `no_stage` - Allows selective disabling of intermediate staging even if configured in sink.

<a id="cli.discover"></a>

#### discover

```python
@app.command(rich_help_panel="Develop")
def discover(
    ctx: typer.Context,
    pipeline: t.Annotated[
        str,
        typer.Argument(help="The pipeline in which to discover resources."),
    ],
    no_quiet: t.Annotated[
        bool,
        typer.Option(
            help="Pipeline stdout is suppressed by default, this disables that."
        ),
    ] = False
) -> None
```

:mag: Dry run a [b blue]Pipeline[/b blue] and enumerates the discovered resources.


**Arguments**:

- `ctx` - The CLI context.
- `pipeline` - The pipeline in which to discover resources.
- `no_quiet` - Whether to suppress the pipeline stdout.

<a id="cli.head"></a>

#### head

```python
@app.command(rich_help_panel="Develop")
def head(ctx: typer.Context,
         pipeline: t.Annotated[str,
                               typer.Argument(
                                   help="The pipeline to inspect.")],
         resource: t.Annotated[str,
                               typer.Argument(
                                   help="The resource to inspect.")],
         n: t.Annotated[int, typer.Option("-n", "--rows")] = 5) -> None
```

:wrench: Prints the first N rows of a [b green]Resource[/b green] within a [b blue]pipeline[/b blue]. Defaults to [cyan]5[/cyan].

This is useful for quickly inspecting data :detective: and verifying that it is coming over the wire correctly.


**Arguments**:

- `ctx` - The CLI context.
- `pipeline` - The pipeline to inspect.
- `resource` - The resource to inspect.
- `n` - The number of rows to print.
  

**Raises**:

- `typer.BadParameter` - If the resource is not found in the pipeline.

<a id="cli.publish"></a>

#### publish

```python
@app.command(rich_help_panel="Core")
def publish(
    ctx: typer.Context,
    sink_to_publisher: t.Annotated[
        str,
        typer.Argument(help="The sink and publisher separated by a colon."),
    ],
    skip_verification: t.Annotated[
        bool,
        typer.
        Option(help="Skip the verification of the publisher dependencies.", ),
    ] = False
) -> t.Any
```

:outbox_tray: [b yellow]Publish[/b yellow] data from a data store to an [violet]External[/violet] system.


**Arguments**:

- `ctx` - The CLI context.
- `sink_to_publisher` - The sink and publisher separated by a colon.
- `skip_verification` - Whether to skip the verification of the publisher dependencies.

<a id="cli.script"></a>

#### script

```python
@app.command(rich_help_panel="Core")
def script(
    ctx: typer.Context,
    script: t.Annotated[str,
                        typer.Argument(help="The script to execute.")],
    quiet: t.Annotated[bool,
                       typer.Option(
                           help="Suppress the script stdout.")] = False
) -> t.Any
```

:hammer: Execute a [b yellow]Script[/b yellow] within the context of the current workspace.


**Arguments**:

- `ctx` - The CLI context.
- `script` - The script to execute.
- `quiet` - Whether to suppress the script stdout.

<a id="cli.notebook"></a>

#### notebook

```python
@app.command(rich_help_panel="Core")
def notebook(
    ctx: typer.Context,
    notebook: t.Annotated[str,
                          typer.Argument(help="The notebook to execute.")],
    params: t.Annotated[
        str,
        typer.Option(
            ...,
            help=
            "The parameters to pass to the notebook as a json formatted string.",
        ),
    ] = "{}"
) -> t.Any
```

:notebook: Execute a [b yellow]Notebook[/b yellow] within the context of the current workspace.


**Arguments**:

- `ctx` - The CLI context.
- `notebook` - The notebook to execute.
- `params` - The parameters to pass to the notebook as a json formatted string.

<a id="cli.jupyter_lab"></a>

#### jupyter\_lab

```python
@app.command(
    rich_help_panel="Utilities",
    context_settings={
        "allow_extra_args": True,
        "ignore_unknown_options": True
    },
)
def jupyter_lab(ctx: typer.Context) -> None
```

:star2: Start a Jupyter Lab server in the context of a workspace.

<a id="cli.spec"></a>

#### spec

```python
@app.command(rich_help_panel="Develop")
def spec(name: _SpecType, json_schema: bool = False) -> None
```

:blue_book: Print the fields for a given spec type.


**Arguments**:

- `name` - The name of the spec to print.
- `json_schema` - Whether to print the JSON schema for the spec.

<a id="cli.schema_dump"></a>

#### schema\_dump

```python
@schema.command("dump")
def schema_dump(
    ctx: typer.Context,
    pipeline_to_sink: t.Annotated[
        str,
        typer.Argument(
            help="The pipeline:sink combination from which to fetch the schema."
        ),
    ],
    format: t.Annotated[_ExportFormat,
                        typer.Option(help="The format to dump the schema in."
                                     )] = _ExportFormat.json
) -> None
```

:computer: Dump the schema of a [b blue]pipeline[/b blue]:[violet]sink[/violet] combination.


**Arguments**:

- `ctx` - The CLI context.
- `pipeline_to_sink` - The pipeline:sink combination from which to fetch the schema.
- `format` - The format to dump the schema in.
  

**Raises**:

- `typer.BadParameter` - If the pipeline or sink are not found.

<a id="cli.schema_edit"></a>

#### schema\_edit

```python
@schema.command("edit")
def schema_edit(ctx: typer.Context, pipeline_to_sink: t.Annotated[
    str,
    typer.Argument(
        help="The pipeline:sink combination from which to fetch the schema."),
]) -> None
```

:pencil: Edit the schema of a [b blue]pipeline[/b blue]:[violet]sink[/violet] combination using the system editor.


**Arguments**:

- `ctx` - The CLI context.
- `pipeline_to_sink` - The pipeline:sink combination from which to fetch the schema.
  

**Raises**:

- `typer.BadParameter` - If the pipeline or sink are not found.

<a id="cli.state_dump"></a>

#### state\_dump

```python
@state.command("dump")
def state_dump(ctx: typer.Context, pipeline_to_sink: t.Annotated[
    str,
    typer.Argument(
        help="The pipeline:sink combination from which to fetch the schema."),
]) -> None
```

:computer: Dump the state of a [b blue]pipeline[/b blue]:[violet]sink[/violet] combination.


**Arguments**:

- `ctx` - The CLI context.
- `pipeline_to_sink` - The pipeline:sink combination from which to fetch the state.
  

**Raises**:

- `typer.BadParameter` - If the pipeline or sink are not found.

<a id="cli.state_edit"></a>

#### state\_edit

```python
@state.command("edit")
def state_edit(ctx: typer.Context, pipeline_to_sink: t.Annotated[
    str,
    typer.Argument(
        help="The pipeline:sink combination from which to fetch the state."),
]) -> None
```

:pencil: Edit the state of a [b blue]pipeline[/b blue]:[violet]sink[/violet] combination using the system editor.


**Arguments**:

- `ctx` - The CLI context.
- `pipeline_to_sink` - The pipeline:sink combination from which to fetch the state.
  

**Raises**:

- `typer.BadParameter` - If the pipeline or sink are not found.

<a id="cli.model_evaluate"></a>

#### model\_evaluate

```python
@model.command("evaluate")
def model_evaluate(
    ctx: typer.Context,
    model: t.Annotated[
        str,
        typer.Argument(
            help="The model to evaluate. Can be prefixed with the gateway."),
    ],
    start: str = typer.Option(
        "1 month ago",
        help=
        "The start time to evaluate the model from. Defaults to 1 month ago.",
    ),
    end: str = typer.Option(
        "now",
        help="The end time to evaluate the model to. Defaults to now.",
    ),
    limit: t.Optional[int] = typer.Option(
        None, help="The number of rows to limit the evaluation to.")
) -> None
```

:bar_chart: Evaluate a [b red]Model[/b red] and print the results. A thin wrapper around `sqlmesh evaluate`


**Arguments**:

- `ctx` - The CLI context.
- `model` - The model to evaluate. Can be prefixed with the gateway.
- `limit` - The number of rows to limit the evaluation to.

<a id="cli.model_render"></a>

#### model\_render

```python
@model.command("render")
def model_render(
    ctx: typer.Context,
    model: t.Annotated[
        str,
        typer.Argument(
            help="The model to evaluate. Can be prefixed with the gateway."),
    ],
    start: str = typer.Option(
        "1 month ago",
        help=
        "The start time to evaluate the model from. Defaults to 1 month ago.",
    ),
    end: str = typer.Option(
        "now",
        help="The end time to evaluate the model to. Defaults to now.",
    ),
    expand: t.List[str] = typer.Option(
        [], help="The referenced models to expand."),
    dialect: t.Optional[str] = typer.Option(
        None, help="The SQL dialect to use for rendering.")
) -> None
```

:bar_chart: Render a [b red]Model[/b red] and print the query. A thin wrapper around `sqlmesh render`


**Arguments**:

- `ctx` - The CLI context.
- `model` - The model to evaluate. Can be prefixed with the gateway.
- `start` - The start time to evaluate the model from. Defaults to 1 month ago.
- `end` - The end time to evaluate the model to. Defaults to now.
- `expand` - The referenced models to expand.
- `dialect` - The SQL dialect to use for rendering.

<a id="cli.model_name"></a>

#### model\_name

```python
@model.command("name")
def model_name(ctx: typer.Context, model: t.Annotated[
    str,
    typer.Argument(
        help=
        "The model to convert the physical name. Can be prefixed with the gateway."
    ),
]) -> None
```

:bar_chart: Get a [b red]Model[/b red]'s physical table name. A thin wrapper around `sqlmesh table_name`


**Arguments**:

- `ctx` - The CLI context.
- `model` - The model to evaluate. Can be prefixed with the gateway.

<a id="cli.model_diff"></a>

#### model\_diff

```python
@model.command("diff")
def model_diff(
    ctx: typer.Context,
    model: t.Annotated[
        str,
        typer.Argument(
            help="The model to evaluate. Can be prefixed with the gateway."),
    ],
    source_target: t.Annotated[
        str,
        typer.Argument(
            help="The source and target environments separated by a colon."),
    ],
    show_sample: bool = typer.Option(
        False, help="Whether to show a sample of the diff.")
) -> None
```

:bar_chart: Compute the diff of a [b red]Model[/b red] across 2 environments. A thin wrapper around `sqlmesh table_diff`


**Arguments**:

- `ctx` - The CLI context.
- `model` - The model to evaluate. Can be prefixed with the gateway.
- `source_target` - The source and target environments separated by a colon.

<a id="cli.model_prototype"></a>

#### model\_prototype

```python
@model.command("prototype")
def model_prototype(
    ctx: typer.Context,
    dependencies: t.List[str] = typer.Option(
        [],
        "-d",
        "--dependencies",
        help="The dependencies to include in the prototype.",
    ),
    start: str = typer.Option(
        "1 month ago",
        help=
        "The start time to evaluate the model from. Defaults to 1 month ago.",
    ),
    end: str = typer.Option(
        "now",
        help="The end time to evaluate the model to. Defaults to now.",
    ),
    limit: int = typer.Option(
        5_000_000,
        help="The number of rows to limit the evaluation to.",
    ))
```

:bar_chart: Prototype a model and save the results to disk.


**Arguments**:

- `ctx` - The CLI context.
- `dependencies` - The dependencies to include in the prototype.
- `start` - The start time to evaluate the model from. Defaults to 1 month ago.
- `end` - The end time to evaluate the model to. Defaults to now.
- `limit` - The number of rows to limit the evaluation to.

<a id="types"></a>

# types

A module for shared types.

<a id="types.monads"></a>

# types.monads

Contains monadic types and functions for working with them.

<a id="types.monads.T"></a>

#### T

The type of the value inside the Monad

<a id="types.monads.U"></a>

#### U

The transformed type of the value inside the Monad

<a id="types.monads.K"></a>

#### K

A known type that is not necessarily the same as T

<a id="types.monads.L"></a>

#### L

A known type that is not necessarily the same as U

<a id="types.monads.E"></a>

#### E

The type of the error inside the Result

<a id="types.monads.TState"></a>

#### TState

The type of the state

<a id="types.monads.TMonad"></a>

#### TMonad

Generic Self type for Monad

<a id="types.monads.Maybe"></a>

## Maybe Objects

```python
class Maybe(Monad[T], abc.ABC)
```

<a id="types.monads.Maybe.pure"></a>

#### pure

```python
@classmethod
def pure(cls, value: K) -> "Maybe[K]"
```

Creates a Maybe with a value.

<a id="types.monads.Maybe.unwrap"></a>

#### unwrap

```python
def unwrap() -> T
```

Unwraps the value of the Maybe.

**Returns**:

  The unwrapped value.

<a id="types.monads.Maybe.unwrap_or"></a>

#### unwrap\_or

```python
def unwrap_or(default: U) -> t.Union[T, U]
```

Tries to unwrap the Maybe, returning a default value if the Maybe is Nothing.

**Arguments**:

- `default` - The value to return if unwrapping Nothing.
  

**Returns**:

  The unwrapped value or the default value.

<a id="types.monads.Maybe.lift"></a>

#### lift

```python
@classmethod
def lift(cls, func: t.Callable[[U],
                               K]) -> t.Callable[["U | Maybe[U]"], "Maybe[K]"]
```

Lifts a function to work within the Maybe monad.

**Arguments**:

- `func` - A function to lift.
  

**Returns**:

  A new function that returns a Maybe value.

<a id="types.monads.Maybe.__iter__"></a>

#### \_\_iter\_\_

```python
def __iter__() -> t.Iterator[T]
```

Allows safely unwrapping the value of the Maybe using a for construct.

<a id="types.monads.Just"></a>

## Just Objects

```python
class Just(Maybe[T])
```

<a id="types.monads.Just.bind"></a>

#### bind

```python
def bind(func: t.Callable[[T], Maybe[U]]) -> Maybe[U]
```

Applies a function to the value inside the Just.

**Arguments**:

- `func` - A function that takes a value of type T and returns a Maybe containing a value of type U.
  

**Returns**:

  The result of applying the function to the value inside the Just.

<a id="types.monads.Just.map"></a>

#### map

```python
def map(func: t.Callable[[T], U]) -> "Maybe[U]"
```

Applies a mapping function to the value inside the Just.

**Arguments**:

- `func` - A function that takes a value of type T and returns a value of type U.
  

**Returns**:

  A new Just containing the result of applying the function to the value inside the Just.

<a id="types.monads.Just.filter"></a>

#### filter

```python
def filter(predicate: t.Callable[[T], bool]) -> Maybe[T]
```

Filters the value inside the Just based on a predicate.

**Arguments**:

- `predicate` - A function that takes a value of type T and returns a boolean.
  

**Returns**:

  A new Just containing the value inside the Just if the predicate holds.

<a id="types.monads.Just.is_just"></a>

#### is\_just

```python
def is_just() -> bool
```

Returns True if the Maybe is a Just.

<a id="types.monads.Just.is_nothing"></a>

#### is\_nothing

```python
def is_nothing() -> bool
```

Returns False if the Maybe is a Just.

<a id="types.monads.Nothing"></a>

## Nothing Objects

```python
class Nothing(Maybe[T])
```

<a id="types.monads.Nothing.bind"></a>

#### bind

```python
def bind(func: t.Callable[[T], Maybe[U]]) -> "Nothing[T]"
```

Applies a function to the value inside the Just.

**Arguments**:

- `func` - A function that takes a value of type T and returns a Maybe containing a value of type U.
  

**Returns**:

  The result of applying the function to the value inside the Just.

<a id="types.monads.Nothing.map"></a>

#### map

```python
def map(func: t.Callable[[T], U]) -> "Nothing[T]"
```

Applies a mapping function to the value inside the Just.

**Arguments**:

- `func` - A function that takes a value of type T and returns a value of type U.
  

**Returns**:

  A new Just containing the result of applying the function to the value inside the Just.

<a id="types.monads.Nothing.filter"></a>

#### filter

```python
def filter(predicate: t.Callable[[T], bool]) -> "Nothing[T]"
```

Filters the value inside the Just based on a predicate.

**Arguments**:

- `predicate` - A function that takes a value of type T and returns a boolean.
  

**Returns**:

  A new Just containing the value inside the Just if the predicate holds.

<a id="types.monads.Nothing.is_just"></a>

#### is\_just

```python
def is_just() -> bool
```

Returns False if the Maybe is a Nothing.

<a id="types.monads.Nothing.is_nothing"></a>

#### is\_nothing

```python
def is_nothing() -> bool
```

Returns True if the Maybe is a Nothing.

<a id="types.monads.Result"></a>

## Result Objects

```python
class Result(Monad[T], t.Generic[T, E])
```

<a id="types.monads.Result.pure"></a>

#### pure

```python
@classmethod
def pure(cls, value: K) -> "Result[K, E]"
```

Creates an Ok with a value.

<a id="types.monads.Result.lift"></a>

#### lift

```python
@classmethod
def lift(
    cls, func: t.Callable[[U], K]
) -> t.Callable[["U | Result[U, Exception]"], "Result[K, Exception]"]
```

Transforms a function to work with arguments and output wrapped in Result monads.

**Arguments**:

- `func` - A function that takes any number of arguments and returns a value of type T.
  

**Returns**:

  A function that takes the same number of unwrapped arguments and returns a Result-wrapped result.

<a id="types.monads.Result.__iter__"></a>

#### \_\_iter\_\_

```python
def __iter__() -> t.Iterator[T]
```

Allows safely unwrapping the value of the Result using a for construct.

<a id="types.monads.Ok"></a>

## Ok Objects

```python
class Ok(Result[T, E])
```

<a id="types.monads.Ok.bind"></a>

#### bind

```python
def bind(func: t.Callable[[T], Result[U, E]]) -> Result[U, E]
```

Applies a function to the result of the Ok.

**Arguments**:

- `func` - A function that takes a value of type T and returns a Result containing a value of type U.
  

**Returns**:

  A new Result containing the result of the original Result after applying the function.

<a id="types.monads.Ok.map"></a>

#### map

```python
def map(func: t.Callable[[T], U]) -> Result[U, E]
```

Applies a mapping function to the result of the Ok.

**Arguments**:

- `func` - A function that takes a value of type T and returns a value of type U.
  

**Returns**:

  A new Ok containing the result of the original Ok after applying the function.

<a id="types.monads.Ok.is_ok"></a>

#### is\_ok

```python
def is_ok() -> bool
```

Returns True if the Result is an Ok.

<a id="types.monads.Ok.is_err"></a>

#### is\_err

```python
def is_err() -> bool
```

Returns False if the Result is an Ok.

<a id="types.monads.Ok.unwrap"></a>

#### unwrap

```python
def unwrap() -> T
```

Unwraps the value of the Ok.

**Returns**:

  The unwrapped value.

<a id="types.monads.Ok.unwrap_or"></a>

#### unwrap\_or

```python
def unwrap_or(default: t.Any) -> T
```

Tries to unwrap the Ok, returning a default value if unwrapping raises an exception.

**Arguments**:

- `default` - The value to return if unwrapping raises an exception.
  

**Returns**:

  The unwrapped value or the default value if an exception is raised.

<a id="types.monads.Ok.unwrap_err"></a>

#### unwrap\_err

```python
def unwrap_err() -> BaseException
```

Raises a ValueError since the Result is an Ok.

<a id="types.monads.Ok.filter"></a>

#### filter

```python
def filter(predicate: t.Callable[[T], bool]) -> Result[T, E]
```

Filters the result of the Ok based on a predicate.

**Arguments**:

- `predicate` - A function that takes a value of type T and returns a boolean.
- `error` - The error to return if the predicate does not hold.
  

**Returns**:

  A new Result containing the result of the original Result if the predicate holds.

<a id="types.monads.Ok.to_parts"></a>

#### to\_parts

```python
def to_parts() -> t.Tuple[T, None]
```

Unpacks the value of the Ok.

<a id="types.monads.Err"></a>

## Err Objects

```python
class Err(Result[T, E])
```

<a id="types.monads.Err.__init__"></a>

#### \_\_init\_\_

```python
def __init__(error: E) -> None
```

Initializes an Err with an error.

**Arguments**:

- `error` - The error to wrap in the Err.

<a id="types.monads.Err.bind"></a>

#### bind

```python
def bind(func: t.Callable[[T], Result[U, E]]) -> "Err[T, E]"
```

Applies a function to the result of the Err.

**Arguments**:

- `func` - A function that takes a value of type T and returns a Result containing a value of type U.
  

**Returns**:

  An Err containing the original error.

<a id="types.monads.Err.map"></a>

#### map

```python
def map(func: t.Callable[[T], U]) -> "Err[T, E]"
```

Applies a mapping function to the result of the Err.

**Arguments**:

- `func` - A function that takes a value of type T and returns a value of type U.
  

**Returns**:

  An Err containing the original error.

<a id="types.monads.Err.is_ok"></a>

#### is\_ok

```python
def is_ok() -> bool
```

Returns False if the Result is an Err.

<a id="types.monads.Err.is_err"></a>

#### is\_err

```python
def is_err() -> bool
```

Returns True if the Result is an Err.

<a id="types.monads.Err.unwrap"></a>

#### unwrap

```python
def unwrap() -> T
```

Raises a ValueError since the Result is an Err.

<a id="types.monads.Err.unwrap_or"></a>

#### unwrap\_or

```python
def unwrap_or(default: U) -> U
```

Returns a default value since the Result is an Err.

**Arguments**:

- `default` - The value to return.
  

**Returns**:

  The default value.

<a id="types.monads.Err.unwrap_err"></a>

#### unwrap\_err

```python
def unwrap_err() -> BaseException
```

Unwraps the error of the Err.

**Returns**:

  The unwrapped error.

<a id="types.monads.Err.filter"></a>

#### filter

```python
def filter(predicate: t.Callable[[T], bool]) -> "Err[T, E]"
```

Filters the result of the Err based on a predicate.

**Arguments**:

- `predicate` - A function that takes a value of type T and returns a boolean.
  

**Returns**:

  An Err containing the original error.

<a id="types.monads.Err.to_parts"></a>

#### to\_parts

```python
def to_parts() -> t.Tuple[None, E]
```

Unpacks the error of the Err.

<a id="types.monads.Promise"></a>

## Promise Objects

```python
class Promise(t.Generic[T], t.Awaitable[T], Monad[T])
```

<a id="types.monads.Promise.__init__"></a>

#### \_\_init\_\_

```python
def __init__(coro_func: t.Callable[P, t.Coroutine[None, None, T]], *args:
             P.args, **kwargs: P.kwargs) -> None
```

Initializes a Promise with a coroutine function.

**Arguments**:

- `coro_func` - A coroutine function that returns a value of type T.
- `args` - Positional arguments to pass to the coroutine function.
- `kwargs` - Keyword arguments to pass to the coroutine function.

<a id="types.monads.Promise.pure"></a>

#### pure

```python
@classmethod
def pure(cls, value: K) -> "Promise[K]"
```

Creates a Promise that is already resolved with a value.

**Arguments**:

- `value` - The value to resolve the Promise with.
  

**Returns**:

  A new Promise that is already resolved with the value.

<a id="types.monads.Promise.__await__"></a>

#### \_\_await\_\_

```python
def __await__()
```

Allows the Promise to be awaited.

<a id="types.monads.Promise.set_result"></a>

#### set\_result

```python
def set_result(result: T) -> None
```

Sets a result on the Promise.

**Arguments**:

- `result` - The result to set on the Promise.

<a id="types.monads.Promise.set_exception"></a>

#### set\_exception

```python
def set_exception(exception: Exception) -> None
```

Sets an exception on the Promise.

**Arguments**:

- `exception` - The exception to set on the Promise.

<a id="types.monads.Promise.bind"></a>

#### bind

```python
def bind(func: t.Callable[[T], "Promise[U]"]) -> "Promise[U]"
```

Applies a function to the result of the Promise.

**Arguments**:

- `func` - A function that takes a value of type T and returns a Promise containing a value of type U.
  

**Returns**:

  A new Promise containing the result of the original Promise after applying the function.

<a id="types.monads.Promise.map"></a>

#### map

```python
def map(func: t.Callable[[T], U]) -> "Promise[U]"
```

Applies a mapping function to the result of the Promise.

**Arguments**:

- `func` - A function that takes a value of type T and returns a value of type U.
  

**Returns**:

  A new Promise containing the result of the original Promise after applying the function.

<a id="types.monads.Promise.then"></a>

#### then

syntactic sugar, equivalent to map

<a id="types.monads.Promise.filter"></a>

#### filter

```python
def filter(predicate: t.Callable[[T], bool]) -> "Promise[T]"
```

Filters the result of the Promise based on a predicate.

**Arguments**:

- `predicate` - A function that takes a value of type T and returns a boolean.
  

**Returns**:

  A new Promise containing the result of the original Promise if the predicate holds.

<a id="types.monads.Promise.unwrap_or"></a>

#### unwrap\_or

```python
def unwrap_or(default: T) -> T
```

Tries to unwrap the Promise, returning a default value if unwrapping raises an exception.

**Arguments**:

- `default` - The value to return if unwrapping raises an exception.
  

**Returns**:

  The unwrapped value or the default value if an exception is raised.

<a id="types.monads.Promise.from_value"></a>

#### from\_value

```python
@classmethod
def from_value(cls, value: T) -> "Promise[T]"
```

Creates a Promise that is already resolved with a value.

**Arguments**:

- `value` - The value to resolve the Promise with.
  

**Returns**:

  A new Promise that is already resolved with the value.

<a id="types.monads.Promise.from_exception"></a>

#### from\_exception

```python
@classmethod
def from_exception(cls, exception: BaseException) -> "Promise[T]"
```

Creates a Promise that is already resolved with an exception.

**Arguments**:

- `exception` - The exception to resolve the Promise with.
  

**Returns**:

  A new Promise that is already resolved with the exception.

<a id="types.monads.Promise.lift"></a>

#### lift

```python
@classmethod
def lift(
        cls,
        func: t.Callable[[U],
                         T]) -> t.Callable[["U | Promise[U]"], "Promise[T]"]
```

Lifts a synchronous function to work within the Promise context,
making it return a Promise of the result and allowing it to be used
with Promise inputs.

**Arguments**:

- `func` - A synchronous function that returns a value of type T.
  

**Returns**:

  A function that, when called, returns a Promise wrapping the result of the original function.

<a id="types.monads.Lazy"></a>

## Lazy Objects

```python
class Lazy(Monad[T])
```

<a id="types.monads.Lazy.__init__"></a>

#### \_\_init\_\_

```python
def __init__(computation: t.Callable[[], T]) -> None
```

Initializes a Lazy monad with a computation that will be executed lazily.

**Arguments**:

- `computation` - A function that takes no arguments and returns a value of type T.

<a id="types.monads.Lazy.pure"></a>

#### pure

```python
@classmethod
def pure(cls, value: T) -> "Lazy[T]"
```

Creates a Lazy monad with a pure value.

<a id="types.monads.Lazy.evaluate"></a>

#### evaluate

```python
def evaluate() -> T
```

Evaluates the computation if it has not been evaluated yet and caches the result.

**Returns**:

  The result of the computation.

<a id="types.monads.Lazy.bind"></a>

#### bind

```python
def bind(func: t.Callable[[T], "Lazy[U]"]) -> "Lazy[U]"
```

Lazily applies a function to the result of the current computation.

**Arguments**:

- `func` - A function that takes a value of type T and returns a Lazy monad containing a value of type U.
  

**Returns**:

  A new Lazy monad containing the result of the computation after applying the function.

<a id="types.monads.Lazy.map"></a>

#### map

```python
def map(func: t.Callable[[T], U]) -> "Lazy[U]"
```

Lazily applies a mapping function to the result of the computation.

**Arguments**:

- `func` - A function that takes a value of type T and returns a value of type U.
  

**Returns**:

  A new Lazy monad containing the result of the computation after applying the function.

<a id="types.monads.Lazy.filter"></a>

#### filter

```python
def filter(predicate: t.Callable[[T], bool]) -> "Lazy[T]"
```

Lazily filters the result of the computation based on a predicate.

**Arguments**:

- `predicate` - A function that takes a value of type T and returns a boolean.
  

**Returns**:

  A new Lazy monad containing the result of the computation if the predicate holds.

<a id="types.monads.Lazy.unwrap"></a>

#### unwrap

```python
def unwrap() -> T
```

Forces evaluation of the computation and returns its result.

**Returns**:

  The result of the computation.

<a id="types.monads.Lazy.unwrap_or"></a>

#### unwrap\_or

```python
def unwrap_or(default: T) -> T
```

Tries to evaluate the computation, returning a default value if evaluation raises an exception.

**Arguments**:

- `default` - The value to return if the computation raises an exception.
  

**Returns**:

  The result of the computation or the default value if an exception is raised.

<a id="types.monads.Lazy.lift"></a>

#### lift

```python
@classmethod
def lift(cls, func: t.Callable[[U],
                               T]) -> t.Callable[["U | Lazy[U]"], "Lazy[T]"]
```

Transforms a function to work with arguments and output wrapped in Lazy monads.

**Arguments**:

- `func` - A function that takes any number of arguments and returns a value of type U.
  

**Returns**:

  A function that takes the same number of Lazy-wrapped arguments and returns a Lazy-wrapped result.

<a id="types.monads.Defer"></a>

#### Defer

Defer is an alias for Lazy

<a id="types.monads.S"></a>

#### S

State type

<a id="types.monads.A"></a>

#### A

Return type

<a id="types.monads.B"></a>

#### B

Transformed type

<a id="types.monads.State"></a>

## State Objects

```python
class State(t.Generic[S, A], Monad[A], abc.ABC)
```

<a id="types.monads.State.lift"></a>

#### lift

```python
@classmethod
def lift(
    cls,
    func: t.Callable[[U],
                     A]) -> t.Callable[["U | State[S, U]"], "State[S, A]"]
```

Lifts a function to work within the State monad.

**Arguments**:

- `func` - A function to lift.

**Returns**:

  A new function that returns a State value.

<a id="types.monads.error"></a>

#### error

noqa: E731

<a id="core"></a>

# core

<a id="core.config"></a>

# core.config

The config module provides a configuration provider for CDF scoped settings.

This allows for the configuration to be accessed and modified in a consistent manner across
the codebase leveraging dlt's configuration provider interface. It also makes all of dlt's
semantics which depend on the configuration providers seamlessly work with CDF's configuration.

<a id="core.config.CdfConfigProvider"></a>

## CdfConfigProvider Objects

```python
class CdfConfigProvider(_ConfigProvider)
```

A configuration provider for CDF scoped settings.

<a id="core.config.CdfConfigProvider.__init__"></a>

#### \_\_init\_\_

```python
def __init__(scope: t.ChainMap[str, t.Any], secret: bool = False) -> None
```

Initialize the provider.

**Arguments**:

- `config` - The configuration ChainMap.

<a id="core.config.CdfConfigProvider.get_value"></a>

#### get\_value

```python
def get_value(key: str, hint: t.Type[t.Any], pipeline_name: str, *sections:
              str) -> t.Tuple[t.Optional[t.Any], str]
```

Get a value from the configuration.

<a id="core.config.CdfConfigProvider.set_value"></a>

#### set\_value

```python
def set_value(key: str, value: t.Any, pipeline_name: str, *sections:
              str) -> None
```

Set a value in the configuration.

<a id="core.config.CdfConfigProvider.name"></a>

#### name

```python
@property
def name() -> str
```

The name of the provider

<a id="core.config.CdfConfigProvider.supports_sections"></a>

#### supports\_sections

```python
@property
def supports_sections() -> bool
```

This provider supports sections

<a id="core.config.CdfConfigProvider.supports_secrets"></a>

#### supports\_secrets

```python
@property
def supports_secrets() -> bool
```

There is no differentiation between secrets and non-secrets for the cdf provider.

Nothing is persisted. Data is available in memory and backed by the dynaconf settings object.

<a id="core.config.CdfConfigProvider.is_writable"></a>

#### is\_writable

```python
@property
def is_writable() -> bool
```

Whether the provider is writable

<a id="core.config.get_config_providers"></a>

#### get\_config\_providers

```python
def get_config_providers(
    scope: t.ChainMap[str, t.Any],
    include_env: bool = True
) -> t.Union[
        t.Tuple[CdfConfigProvider, CdfConfigProvider],
        t.Tuple[EnvironProvider, CdfConfigProvider, CdfConfigProvider],
]
```

Get the configuration providers for the given scope.

<a id="core.config.inject_configuration"></a>

#### inject\_configuration

```python
@contextmanager
def inject_configuration(
        scope: t.ChainMap[str, t.Any],
        include_env: bool = True) -> t.Iterator[t.Mapping[str, t.Any]]
```

Inject the configuration provider into the context

This allows dlt.config and dlt.secrets to access the scope configuration. Furthermore
it makes the scope configuration available throughout dlt where things such as extract,
normalize, and load settings can be specified.

<a id="core.filesystem"></a>

# core.filesystem

A central interface for filesystems thinly wrapping fsspec.

<a id="core.filesystem.FilesystemAdapter"></a>

## FilesystemAdapter Objects

```python
class FilesystemAdapter()
```

Wraps an fsspec filesystem.

The filesystem is lazily loaded. Certain methods are intercepted to include cdf-specific logic. Helper
methods are provided for specific operations.

<a id="core.filesystem.FilesystemAdapter.__init__"></a>

#### \_\_init\_\_

```python
@with_config(sections=("filesystem", ))
def __init__(uri: PathLike = dlt.config.value,
             root: t.Optional[PathLike] = None,
             options: t.Optional[t.Dict[str, t.Any]] = None) -> None
```

Load a filesystem from a provider and kwargs.

**Arguments**:

- `uri` - The filesystem URI.
- `options` - The filesystem provider kwargs.

<a id="core.filesystem.FilesystemAdapter.__getattr__"></a>

#### \_\_getattr\_\_

```python
def __getattr__(name: str) -> t.Any
```

Proxy attribute access to the wrapped filesystem.

<a id="core.filesystem.FilesystemAdapter.__getitem__"></a>

#### \_\_getitem\_\_

```python
def __getitem__(value: str) -> t.Any
```

Get a path from the filesystem.

<a id="core.filesystem.FilesystemAdapter.__setitem__"></a>

#### \_\_setitem\_\_

```python
def __setitem__(key: str, value: t.Any) -> None
```

Set a path in the filesystem.

<a id="core.filesystem.FilesystemAdapter.open"></a>

#### open

```python
def open(path: PathLike, mode: str = "r", **kwargs: t.Any) -> t.Any
```

Open a file from the filesystem.

**Arguments**:

- `path` - The path to the file.
- `mode` - The file mode.
- `kwargs` - Additional kwargs.
  

**Returns**:

  The file handle.

<a id="core.constants"></a>

# core.constants

Constants used by CDF.

<a id="core.constants.CDF_ENVIRONMENT"></a>

#### CDF\_ENVIRONMENT

Environment variable to set the environment of the project.

<a id="core.constants.DEFAULT_ENVIRONMENT"></a>

#### DEFAULT\_ENVIRONMENT

Default environment for the project.

<a id="core.constants.CDF_MAIN"></a>

#### CDF\_MAIN

A sentinel value that will match the __name__ attribute of a module being executed by CDF.

<a id="core.constants.CDF_LOG_LEVEL"></a>

#### CDF\_LOG\_LEVEL

Environment variable to set the log level of the project.

<a id="core.runtime"></a>

# core.runtime

<a id="core.runtime.common"></a>

# core.runtime.common

<a id="core.runtime.common.with_activate_project"></a>

#### with\_activate\_project

```python
def with_activate_project(func: t.Callable[P, T]) -> t.Callable[P, T]
```

Attempt to inject the Project associated with the first argument into cdf.context.

**Arguments**:

- `func` - The function to decorate.
  

**Returns**:

  The decorated function.

<a id="core.runtime.pipeline"></a>

# core.runtime.pipeline

The runtime pipeline module is responsible for executing pipelines from pipeline specifications.

It performs the following functions:
- Injects the runtime context into the pipeline.
- Executes the pipeline.
- Captures metrics during extract.
- Intercepts sources during extract. (if specified, this makes the pipeline a no-op)
- Applies transformations to sources during extract.
- Stages data if a staging location is provided and enabled in the runtime context.
- Forces replace disposition if specified in the runtime context.
- Filters resources based on glob patterns.
- Logs a warning if dataset_name is provided in the runtime context. (since we want to manage it)
- Creates a cdf pipeline from a dlt pipeline.

<a id="core.runtime.pipeline.pipeline"></a>

#### pipeline

Gets the active pipeline or creates a new one with the given arguments.

<a id="core.runtime.pipeline.RuntimePipeline"></a>

## RuntimePipeline Objects

```python
class RuntimePipeline(Pipeline)
```

Overrides certain methods of the dlt pipeline to allow for cdf specific behavior.

<a id="core.runtime.pipeline.RuntimePipeline.configure"></a>

#### configure

```python
def configure(dry_run: bool = False,
              force_replace: bool = False,
              select: t.Optional[t.List[str]] = None,
              exclude: t.Optional[t.List[str]] = None) -> "RuntimePipeline"
```

Configures options which affect the behavior of the pipeline at runtime.

**Arguments**:

- `dry_run` - Whether to run the pipeline in dry run mode.
- `force_replace` - Whether to force replace disposition.
- `select` - A list of glob patterns to select resources.
- `exclude` - A list of glob patterns to exclude resources.
  

**Returns**:

- `RuntimePipeline` - The pipeline with source hooks configured.

<a id="core.runtime.pipeline.RuntimePipeline.force_replace"></a>

#### force\_replace

```python
@property
def force_replace() -> bool
```

Whether to force replace disposition.

<a id="core.runtime.pipeline.RuntimePipeline.dry_run"></a>

#### dry\_run

```python
@property
def dry_run() -> bool
```

Dry run mode.

<a id="core.runtime.pipeline.RuntimePipeline.metric_accumulator"></a>

#### metric\_accumulator

```python
@property
def metric_accumulator() -> t.Mapping[str, t.Any]
```

A container for accumulating metrics during extract.

<a id="core.runtime.pipeline.RuntimePipeline.source_hooks"></a>

#### source\_hooks

```python
@property
def source_hooks(
) -> t.List[t.Callable[[dlt.sources.DltSource], dlt.sources.DltSource]]
```

The source hooks for the pipeline.

<a id="core.runtime.pipeline.RuntimePipeline.tracked_sources"></a>

#### tracked\_sources

```python
@property
def tracked_sources() -> t.Set[dlt.sources.DltSource]
```

The sources tracked by the pipeline.

<a id="core.runtime.pipeline.PipelineResult"></a>

## PipelineResult Objects

```python
class PipelineResult(t.NamedTuple)
```

The result of executing a pipeline specification.

<a id="core.runtime.pipeline.execute_pipeline_specification"></a>

#### execute\_pipeline\_specification

```python
@with_activate_project
def execute_pipeline_specification(
        pipe_spec: PipelineSpecification,
        sink_spec: t.Union[
            TDestinationReferenceArg,
            t.Tuple[TDestinationReferenceArg,
                    t.Optional[TDestinationReferenceArg]],
            SinkSpecification,
        ],
        select: t.Optional[t.List[str]] = None,
        exclude: t.Optional[t.List[str]] = None,
        force_replace: bool = False,
        dry_run: bool = False,
        enable_stage: bool = True,
        quiet: bool = False,
        **pipeline_options: t.Any) -> M.Result[PipelineResult, Exception]
```

Executes a pipeline specification.

**Arguments**:

- `pipe_spec` - The pipeline specification.
- `sink_spec` - The destination where the pipeline will write data.
- `select` - A list of glob patterns to select resources.
- `exclude` - A list of glob patterns to exclude resources.
- `force_replace` - Whether to force replace disposition.
- `dry_run` - Whether to run the pipeline in dry run mode.
- `enable_stage` - Whether to enable staging. If disabled, staging will be ignored.
- `quiet` - Whether to suppress output.
- `pipeline_options` - Additional dlt.pipeline constructor arguments.
  

**Returns**:

  M.Result[PipelineResult, Exception]: The result of executing the pipeline specification.

<a id="core.runtime.publisher"></a>

# core.runtime.publisher

The runtime publisher module is responsible for executing publishers from publisher specifications.

It performs the following functions:
- Validates the dependencies of the publisher exist.
- Verifies the dependencies are up-to-date.
- Executes the publisher script.

<a id="core.runtime.publisher.execute_publisher_specification"></a>

#### execute\_publisher\_specification

```python
@with_activate_project
def execute_publisher_specification(
    spec: PublisherSpecification,
    transform_ctx: sqlmesh.Context,
    skip_verification: bool = False
) -> M.Result[t.Dict[str, t.Any], Exception]
```

Execute a publisher specification.

**Arguments**:

- `spec` - The publisher specification to execute.
- `transform_ctx` - The SQLMesh context to use for execution.
- `skip_verification` - Whether to skip the verification of the publisher dependencies.

<a id="core.runtime.notebook"></a>

# core.runtime.notebook

The runtime notebook module is responsible for executing notebooks from notebook specifications.

It performs the following functions:
- Executes the notebook.
- Writes the output to a designated location in a storage provider.
- Cleans up the rendered notebook if required.

<a id="core.runtime.notebook.execute_notebook_specification"></a>

#### execute\_notebook\_specification

```python
@with_activate_project
def execute_notebook_specification(
        spec: NotebookSpecification,
        **params: t.Any) -> M.Result["NotebookNode", Exception]
```

Execute a notebook specification.

**Arguments**:

- `spec` - The notebook specification to execute.
- `storage` - The filesystem to use for persisting the output.
- `**params` - The parameters to pass to the notebook. Overrides the notebook spec parameters.

<a id="core.runtime.script"></a>

# core.runtime.script

The runtime script module is responsible for executing scripts from script specifications.

It performs the following functions:
- Executes the script.
- Optionally captures stdout and returns it as a string.

<a id="core.runtime.script.execute_script_specification"></a>

#### execute\_script\_specification

```python
@with_activate_project
def execute_script_specification(
    spec: ScriptSpecification,
    capture_stdout: bool = False
) -> t.Union[M.Result[t.Dict[str, t.Any], Exception], M.Result[str,
                                                               Exception]]
```

Execute a script specification.

**Arguments**:

- `spec` - The script specification to execute.
- `capture_stdout` - Whether to capture stdout and return it. False returns an empty string.

<a id="core.logger"></a>

# core.logger

Logger for CDF

<a id="core.logger.LOGGER"></a>

#### LOGGER

CDF logger instance.

<a id="core.logger.LOG_LEVEL"></a>

#### LOG\_LEVEL

The active log level for CDF.

<a id="core.logger.configure"></a>

#### configure

```python
def configure(level: int | str = logging.INFO) -> None
```

Configure logging.

**Arguments**:

- `level` _int, optional_ - Logging level. Defaults to logging.INFO.

<a id="core.logger.create"></a>

#### create

```python
def create(name: str | None = None) -> CDFLoggerAdapter | logging.Logger
```

Get or create a logger.

**Arguments**:

- `name` _str, optional_ - The name of the logger. If None, the package logger is
  returned. Defaults to None. If a name is provided, a child logger is
  created.
  

**Returns**:

  The logger.

<a id="core.logger.log_level"></a>

#### log\_level

```python
def log_level() -> str
```

Returns current log level

<a id="core.logger.set_level"></a>

#### set\_level

```python
def set_level(level: int | str) -> None
```

Set the package log level.

**Arguments**:

- `level` _int | str_ - The new log level.
  

**Raises**:

- `ValueError` - If the log level is not valid.

<a id="core.logger.suppress_and_warn"></a>

#### suppress\_and\_warn

```python
@contextlib.contextmanager
def suppress_and_warn() -> t.Iterator[None]
```

Suppresses exception and logs it as warning

<a id="core.logger.mute"></a>

#### mute

```python
@contextlib.contextmanager
def mute() -> t.Iterator[None]
```

Mute the logger.

<a id="core.logger.__getattr__"></a>

#### \_\_getattr\_\_

```python
def __getattr__(name: str) -> "LogMethod"
```

Get a logger method from the package logger.

<a id="core.logger.apply_patches"></a>

#### apply\_patches

```python
def apply_patches() -> None
```

Apply logger patches.

<a id="core.specification.sink"></a>

# core.specification.sink

<a id="core.specification.sink.SinkSpecification"></a>

## SinkSpecification Objects

```python
class SinkSpecification(PythonScript)
```

A sink specification.

<a id="core.specification.sink.SinkSpecification.ingest_config"></a>

#### ingest\_config

The variable which holds the ingest configuration (a dlt destination).

<a id="core.specification.sink.SinkSpecification.stage_config"></a>

#### stage\_config

The variable which holds the staging configuration (a dlt destination).

<a id="core.specification.sink.SinkSpecification.transform_config"></a>

#### transform\_config

The variable which holds the transform configuration (a sqlmesh config).

<a id="core.specification.sink.SinkSpecification.get_ingest_config"></a>

#### get\_ingest\_config

```python
def get_ingest_config() -> t.Tuple[Destination, t.Optional[Destination]]
```

Get the ingest configuration.

<a id="core.specification.sink.SinkSpecification.get_transform_config"></a>

#### get\_transform\_config

```python
def get_transform_config() -> GatewayConfig
```

Get the transform configuration.

<a id="core.specification.sink.SinkSpecification.ingest"></a>

#### ingest

```python
@property
def ingest() -> Destination
```

The ingest destination.

<a id="core.specification.sink.SinkSpecification.stage"></a>

#### stage

```python
@property
def stage() -> t.Optional[Destination]
```

The stage destination.

<a id="core.specification.sink.SinkSpecification.transform"></a>

#### transform

```python
@property
def transform() -> GatewayConfig
```

The transform configuration.

<a id="core.specification"></a>

# core.specification

<a id="core.specification.model"></a>

# core.specification.model

<a id="core.specification.pipeline"></a>

# core.specification.pipeline

The spec classes for continuous data framework pipelines.

<a id="core.specification.pipeline.PipelineMetricSpecification"></a>

## PipelineMetricSpecification Objects

```python
class PipelineMetricSpecification(PythonEntrypoint)
```

Defines metrics which can be captured during pipeline execution

<a id="core.specification.pipeline.PipelineMetricSpecification.options"></a>

#### options

Kwargs to pass to the metric function.

This assumes the metric is a callable which accepts kwargs and returns a metric
interface. If the metric is not parameterized, this should be left empty.

<a id="core.specification.pipeline.PipelineMetricSpecification.func"></a>

#### func

```python
@property
def func() -> MetricInterface
```

A typed property to return the metric function

<a id="core.specification.pipeline.PipelineMetricSpecification.__call__"></a>

#### \_\_call\_\_

```python
def __call__(resource: dlt.sources.DltResource,
             state: MetricStateContainer) -> None
```

Adds a metric aggregator to a resource

<a id="core.specification.pipeline.InlineMetricSpecifications"></a>

#### InlineMetricSpecifications

Mapping of resource name glob patterns to metric specs

<a id="core.specification.pipeline.PipelineFilterSpecification"></a>

## PipelineFilterSpecification Objects

```python
class PipelineFilterSpecification(PythonEntrypoint)
```

Defines filters which can be applied to pipeline execution

<a id="core.specification.pipeline.PipelineFilterSpecification.options"></a>

#### options

Kwargs to pass to the filter function.

This assumes the filter is a callable which accepts kwargs and returns a filter
interface. If the filter is already a filter interface, this should be left empty.

<a id="core.specification.pipeline.PipelineFilterSpecification.func"></a>

#### func

```python
@property
def func() -> FilterInterface
```

A typed property to return the filter function

<a id="core.specification.pipeline.PipelineFilterSpecification.__call__"></a>

#### \_\_call\_\_

```python
def __call__(resource: dlt.sources.DltResource) -> None
```

Adds a filter to a resource

<a id="core.specification.pipeline.InlineFilterSpecifications"></a>

#### InlineFilterSpecifications

Mapping of resource name glob patterns to filter specs

<a id="core.specification.pipeline.PipelineSpecification"></a>

## PipelineSpecification Objects

```python
class PipelineSpecification(PythonScript, Schedulable)
```

A pipeline specification.

<a id="core.specification.pipeline.PipelineSpecification.metrics"></a>

#### metrics

A dict of resource name glob patterns to metric definitions.

Metrics are captured on a per resource basis during pipeline execution and are
accumulated into the metric_state dict. The metric definitions are callables that
take the current item and the current metric value and return the new metric value.

<a id="core.specification.pipeline.PipelineSpecification.filters"></a>

#### filters

A dict of resource name glob patterns to filter definitions.

Filters are applied on a per resource basis during pipeline execution. The filter
definitions are callables that take the current item and return a boolean indicating
whether the item should be filtered out.

<a id="core.specification.pipeline.PipelineSpecification.dataset_name"></a>

#### dataset\_name

The name of the dataset associated with the pipeline.

Defaults to the versioned name. This string is formatted with the pipeline name, version, meta, and tags.

<a id="core.specification.pipeline.PipelineSpecification.options"></a>

#### options

Options available in pipeline scoped dlt config resolution.

<a id="core.specification.pipeline.PipelineSpecification.persist_extract_package"></a>

#### persist\_extract\_package

Whether to persist the extract package in the project filesystem.

<a id="core.specification.pipeline.PipelineSpecification.inject_metrics_and_filters"></a>

#### inject\_metrics\_and\_filters

```python
def inject_metrics_and_filters(
        source: dlt.sources.DltSource,
        container: MetricStateContainer) -> dlt.sources.DltSource
```

Apply metrics and filters defined by the specification to a source.

For a source to conform to the specification, it must have this method applied to it. You
can manipulate sources without this method, but the metrics and filters will not be applied.

**Arguments**:

- `source` - The source to apply metrics and filters to.
- `container` - The container to store metric state in. This is mutated during execution.
  

**Returns**:

- `dlt.sources.DltSource` - The source with metrics and filters applied.

<a id="core.specification.pipeline.PipelineSpecification.create_pipeline"></a>

#### create\_pipeline

```python
def create_pipeline(klass: t.Type[TPipeline] = dlt.Pipeline,
                    **kwargs: t.Any) -> TPipeline
```

Convert the pipeline specification to a dlt pipeline object.

This is a convenience method to create a dlt pipeline object from the specification. The
dlt pipeline is expected to use the name and dataset name from the specification. This
is what allows declarative definitions to be associated with runtime artifacts.

**Arguments**:

- `klass` _t.Type[TPipeline], optional_ - The pipeline class to use. Defaults to dlt.Pipeline.
- `**kwargs` - Additional keyword arguments to pass to the dlt.pipeline constructor.
  

**Returns**:

- `TPipeline` - The dlt pipeline object.

<a id="core.specification.publisher"></a>

# core.specification.publisher

<a id="core.specification.publisher.PublisherSpecification"></a>

## PublisherSpecification Objects

```python
class PublisherSpecification(PythonScript, Schedulable)
```

A publisher specification.

<a id="core.specification.publisher.PublisherSpecification.depends_on"></a>

#### depends\_on

The dependencies of the publisher expressed as fully qualified names of SQLMesh tables.

<a id="core.specification.notebook"></a>

# core.specification.notebook

<a id="core.specification.notebook.NotebookSpecification"></a>

## NotebookSpecification Objects

```python
class NotebookSpecification(WorkspaceComponent, InstallableRequirements)
```

A sink specification.

<a id="core.specification.notebook.NotebookSpecification.storage_path"></a>

#### storage\_path

The path to write the output notebook to for long term storage.

Uses the configured Project fs provider. This may be gcs, s3, etc.

This is a format string which will be formatted with the following variables:
- name: The name of the notebook.
- date: The current date.
- timestamp: An ISO formatted timestamp.
- epoch: The current epoch time.
- params: A dict of the resolved parameters passed to the notebook.

<a id="core.specification.notebook.NotebookSpecification.parameters"></a>

#### parameters

Parameters to pass to the notebook when running.

<a id="core.specification.notebook.NotebookSpecification.gc_duration"></a>

#### gc\_duration

The duration in seconds to keep the locally rendered notebook in the `_rendered` folder.

Rendered notebooks are written to the `_rendered` folder of the notebook's parent directory.
That folder is not intended to be a permanent storage location. This setting controls how long
rendered notebooks are kept before being garbage collected. The default is 3 days. Set to 0 to
clean up immediately after execution. Set to -1 to never clean up.

<a id="core.specification.script"></a>

# core.specification.script

<a id="core.specification.script.ScriptSpecification"></a>

## ScriptSpecification Objects

```python
class ScriptSpecification(PythonScript, Schedulable)
```

A script specification.

<a id="core.specification.base"></a>

# core.specification.base

Base specification classes for continuous data framework components

<a id="core.specification.base.BaseComponent"></a>

## BaseComponent Objects

```python
class BaseComponent(pydantic.BaseModel)
```

A component specification.

Components are the building blocks of a data platform. They declaratively describe
the functions within a workspace which extract, load, transform, and publish data.

<a id="core.specification.base.BaseComponent.name"></a>

#### name

The name of the component. Must be unique within the workspace.

<a id="core.specification.base.BaseComponent.version"></a>

#### version

The version of the component.

Used internally to version datasets and serves as an external signal to dependees that something
has changed in a breaking way. All components are versioned.

<a id="core.specification.base.BaseComponent.owner"></a>

#### owner

The owners of the component.

<a id="core.specification.base.BaseComponent.description"></a>

#### description

The description of the component.

This should help users understand the purpose of the component. For scripts and entrypoints, we
will attempt to extract the relevant docstring.

<a id="core.specification.base.BaseComponent.tags"></a>

#### tags

Tags for this component used for component queries and integrations.

<a id="core.specification.base.BaseComponent.enabled"></a>

#### enabled

Whether this component is enabled. Respected in cdf operations.

<a id="core.specification.base.BaseComponent.meta"></a>

#### meta

Arbitrary user-defined metadata for this component.

Used for user-specific integrations and automation.

<a id="core.specification.base.BaseComponent.__eq__"></a>

#### \_\_eq\_\_

```python
def __eq__(other: t.Any) -> bool
```

Check if two components are equal.

<a id="core.specification.base.BaseComponent.__hash__"></a>

#### \_\_hash\_\_

```python
def __hash__() -> int
```

Hash the component.

<a id="core.specification.base.BaseComponent.workspace"></a>

#### workspace

```python
@property
def workspace() -> "Workspace"
```

Get the workspace containing the component.

<a id="core.specification.base.BaseComponent.has_workspace_association"></a>

#### has\_workspace\_association

```python
@property
def has_workspace_association() -> bool
```

Check if the component has a workspace association.

<a id="core.specification.base.BaseComponent.versioned_name"></a>

#### versioned\_name

```python
@property
def versioned_name() -> str
```

Get the versioned name of the component.

<a id="core.specification.base.BaseComponent.owners"></a>

#### owners

```python
@property
def owners() -> t.List[str]
```

Get the owners.

<a id="core.specification.base.BaseComponent.__getitem__"></a>

#### \_\_getitem\_\_

```python
def __getitem__(key: str) -> t.Any
```

Get a field from the component.

<a id="core.specification.base.WorkspaceComponent"></a>

## WorkspaceComponent Objects

```python
class WorkspaceComponent(BaseComponent)
```

A component within a workspace.

<a id="core.specification.base.WorkspaceComponent.component_path"></a>

#### component\_path

The path to the component within the workspace folder.

<a id="core.specification.base.WorkspaceComponent.root_path"></a>

#### root\_path

The base path from which to resolve the component path.

This is typically the union of the project path and the workspace path but
for standalone components (components created programmatically outside the
context of the cdf taxonomy), it should be set to either the current working
directory (default) or the system root.

<a id="core.specification.base.WorkspaceComponent.path"></a>

#### path

```python
@property
def path() -> Path
```

Get the path to the component.

<a id="core.specification.base.Schedulable"></a>

## Schedulable Objects

```python
class Schedulable(pydantic.BaseModel)
```

A mixin for schedulable components.

<a id="core.specification.base.Schedulable.cron_string"></a>

#### cron\_string

A cron expression for scheduling the primary action associated with the component.

This is intended to be leveraged by libraries like Airflow.

<a id="core.specification.base.Schedulable.cron"></a>

#### cron

```python
@property
def cron() -> t.Optional[croniter]
```

Get the croniter instance.

<a id="core.specification.base.Schedulable.next_run"></a>

#### next\_run

```python
def next_run() -> t.Optional[int]
```

Get the next run time for the component.

<a id="core.specification.base.Schedulable.is_due"></a>

#### is\_due

```python
def is_due() -> bool
```

Check if the component is due to run.

<a id="core.specification.base.InstallableRequirements"></a>

## InstallableRequirements Objects

```python
class InstallableRequirements(pydantic.BaseModel)
```

A mixin for components that support installation of requirements.

<a id="core.specification.base.InstallableRequirements.requirements"></a>

#### requirements

The requirements for the component.

<a id="core.specification.base.InstallableRequirements.install_requirements"></a>

#### install\_requirements

```python
def install_requirements() -> None
```

Install the component.

<a id="core.specification.base.PythonScript"></a>

## PythonScript Objects

```python
class PythonScript(WorkspaceComponent, InstallableRequirements)
```

A python script component.

<a id="core.specification.base.PythonScript.auto_install"></a>

#### auto\_install

Whether to automatically install the requirements for the script.

Useful for leaner Docker images which defer certain component dep installs to runtime.

<a id="core.specification.base.PythonScript.package"></a>

#### package

```python
def package(outputdir: str) -> None
```

Package the component.

<a id="core.specification.base.PythonScript.main"></a>

#### main

```python
@property
def main() -> t.Callable[[], t.Dict[str, t.Any]]
```

Get the entrypoint function.

<a id="core.specification.base.PythonScript.__call__"></a>

#### \_\_call\_\_

```python
def __call__() -> t.Dict[str, t.Any]
```

Run the script.

<a id="core.specification.base.PythonEntrypoint"></a>

## PythonEntrypoint Objects

```python
class PythonEntrypoint(BaseComponent, InstallableRequirements)
```

A python entrypoint component.

<a id="core.specification.base.PythonEntrypoint.entrypoint"></a>

#### entrypoint

The entrypoint of the component in the format module:func.

<a id="core.specification.base.PythonEntrypoint.main"></a>

#### main

```python
@property
def main() -> t.Callable[..., t.Any]
```

Get the entrypoint function.

<a id="core.specification.base.PythonEntrypoint.__call__"></a>

#### \_\_call\_\_

```python
def __call__(*args: t.Any, **kwargs: t.Any) -> t.Any
```

Run the entrypoint.

<a id="core.specification.base.CanExecute"></a>

## CanExecute Objects

```python
class CanExecute(t.Protocol)
```

A protocol specifying the minimum interface executable components satisfy.

<a id="core.context"></a>

# core.context

The context module provides thread-safe context variables and injection mechanisms.

It facilitates communication between specifications and runtime modules.

<a id="core.context.active_project"></a>

#### active\_project

The active workspace context variable.

The allows the active workspace to be passed to user-defined scripts. The workspace
has a reference to the project configuration and filesystem.

<a id="core.context.active_pipeline"></a>

#### active\_pipeline

Stores the active pipeline.

This is the primary mechanism to pass a configured pipeline to user-defined scripts.

<a id="core.context.debug_mode"></a>

#### debug\_mode

The debug mode context variable.

Allows us to mutate certain behaviors in the runtime based on the debug mode. User can
optionally introspect this.

<a id="core.context.extract_limit"></a>

#### extract\_limit

The extract limit context variable.

Lets us set a limit on the number of items to extract from a source. This variable
can be introspected by user-defined scripts to optimize for partial extraction.

<a id="core.project"></a>

# core.project

The project module provides a way to define a project and its workspaces.

Everything in CDF is described via a simple configuration structure. We parse this configuration
using dynaconf which provides a simple way to load configuration from various sources such as
environment variables, YAML, TOML, JSON, and Python files. It also provides many other features
such as loading .env files, env-specific configuration, templating via @ tokens, and more. The
configuration is then validated with pydantic to ensure it is correct and to give us well defined
types to work with. The underlying dynaconf settings object is stored in the `wrapped` attribute
of the Project and Workspace settings objects. This allows us to access the raw configuration
values if needed. ChainMaps are used to provide a scoped view of the configuration. This enables
a powerful layering mechanism where we can override configuration values at different levels.
Finally, we provide a context manager to inject the project configuration into the dlt context
which allows us to access the configuration throughout the dlt codebase and in data pipelines.

**Example**:

  
```toml
# cdf.toml
[default]
name = "cdf-example"
workspaces = ["alex"]
filesystem.uri = "file://_storage"
feature_flags.provider = "filesystem"
feature_flags.filename = "feature_flags.json"

[prod]
filesystem.uri = "gcs://bucket/path"
```
  
```toml
# alex/cdf.toml
[pipelines.us_cities] # alex/pipelines/us_cities_pipeline.py
version = 1
dataset_name = "us_cities_v0_{version}"
description = "Get US city data"
options.full_refresh = false
options.runtime.dlthub_telemetry = false
```

<a id="core.project._BaseSettings"></a>

## \_BaseSettings Objects

```python
class _BaseSettings(pydantic.BaseModel)
```

A base model for CDF settings

<a id="core.project._BaseSettings.is_newer_than"></a>

#### is\_newer\_than

```python
def is_newer_than(other: "Project") -> bool
```

Check if the model is newer than another model

<a id="core.project._BaseSettings.is_older_than"></a>

#### is\_older\_than

```python
def is_older_than(other: "Project") -> bool
```

Check if the model is older than another model

<a id="core.project._BaseSettings.model_dump"></a>

#### model\_dump

```python
def model_dump(**kwargs: t.Any) -> t.Dict[str, t.Any]
```

Dump the model to a dictionary

<a id="core.project.FilesystemConfig"></a>

## FilesystemConfig Objects

```python
class FilesystemConfig(_BaseSettings)
```

Configuration for a filesystem provider

<a id="core.project.FilesystemConfig.uri"></a>

#### uri

The filesystem URI

This is based on fsspec. See https://filesystem-spec.readthedocs.io/en/latest/index.html
This supports all filesystems supported by fsspec as well as filesystem chaining.

<a id="core.project.FilesystemConfig.options_"></a>

#### options\_

The filesystem options

Options are passed to the filesystem provider as keyword arguments.

<a id="core.project.FilesystemConfig.options"></a>

#### options

```python
@property
def options() -> t.Dict[str, t.Any]
```

Get the filesystem options as a dictionary

<a id="core.project.FilesystemConfig.project"></a>

#### project

```python
@property
def project() -> "Project"
```

Get the project this configuration belongs to

<a id="core.project.FilesystemConfig.has_project_association"></a>

#### has\_project\_association

```python
@property
def has_project_association() -> bool
```

Check if the configuration is associated with a project

<a id="core.project.FilesystemConfig.get_adapter"></a>

#### get\_adapter

```python
def get_adapter() -> M.Result[FilesystemAdapter, Exception]
```

Get a filesystem adapter

<a id="core.project.FeatureFlagProviderType"></a>

## FeatureFlagProviderType Objects

```python
class FeatureFlagProviderType(str, Enum)
```

The feature flag provider

<a id="core.project.BaseFeatureFlagConfig"></a>

## BaseFeatureFlagConfig Objects

```python
class BaseFeatureFlagConfig(_BaseSettings)
```

Base configuration for a feature flags provider

<a id="core.project.BaseFeatureFlagConfig.provider"></a>

#### provider

The feature flags provider

<a id="core.project.BaseFeatureFlagConfig.project"></a>

#### project

```python
@property
def project() -> "Project"
```

Get the project this configuration belongs to

<a id="core.project.BaseFeatureFlagConfig.has_project_association"></a>

#### has\_project\_association

```python
@property
def has_project_association() -> bool
```

Check if the configuration is associated with a project

<a id="core.project.BaseFeatureFlagConfig.get_adapter"></a>

#### get\_adapter

```python
def get_adapter(**kwargs: t.Any
                ) -> M.Result[AbstractFeatureFlagAdapter, Exception]
```

Get a handle to the feature flag adapter

<a id="core.project.FilesystemFeatureFlagConfig"></a>

## FilesystemFeatureFlagConfig Objects

```python
class FilesystemFeatureFlagConfig(BaseFeatureFlagConfig)
```

Configuration for a feature flags provider that uses the configured filesystem

<a id="core.project.FilesystemFeatureFlagConfig.provider"></a>

#### provider

The feature flags provider

<a id="core.project.FilesystemFeatureFlagConfig.filename"></a>

#### filename

The feature flags filename.

This is a format string that can include the following variables:
- `name`: The project name
- `workspace`: The workspace name
- `environment`: The environment name
- `source`: The source name
- `resource`: The resource name
- `version`: The version number of the component

<a id="core.project.HarnessFeatureFlagConfig"></a>

## HarnessFeatureFlagConfig Objects

```python
class HarnessFeatureFlagConfig(BaseFeatureFlagConfig)
```

Configuration for a feature flags provider that uses the Harness API

<a id="core.project.HarnessFeatureFlagConfig.provider"></a>

#### provider

The feature flags provider

<a id="core.project.HarnessFeatureFlagConfig.api_key"></a>

#### api\_key

The harness API key. Get it from your user settings

<a id="core.project.HarnessFeatureFlagConfig.sdk_key"></a>

#### sdk\_key

The harness SDK key. Get it from the environment management page of the FF module

<a id="core.project.HarnessFeatureFlagConfig.account"></a>

#### account

The harness account ID. We will attempt to read it from the environment if not provided.

<a id="core.project.HarnessFeatureFlagConfig.organization"></a>

#### organization

The harness organization ID. We will attempt to read it from the environment if not provided.

<a id="core.project.HarnessFeatureFlagConfig.project_"></a>

#### project\_

The harness project ID. We will attempt to read it from the environment if not provided.

<a id="core.project.LaunchDarklyFeatureFlagSettings"></a>

## LaunchDarklyFeatureFlagSettings Objects

```python
class LaunchDarklyFeatureFlagSettings(BaseFeatureFlagConfig)
```

Configuration for a feature flags provider that uses the LaunchDarkly API

<a id="core.project.LaunchDarklyFeatureFlagSettings.provider"></a>

#### provider

The feature flags provider

<a id="core.project.LaunchDarklyFeatureFlagSettings.api_key"></a>

#### api\_key

The LaunchDarkly API key. Get it from your user settings

<a id="core.project.SplitFeatureFlagSettings"></a>

## SplitFeatureFlagSettings Objects

```python
class SplitFeatureFlagSettings(BaseFeatureFlagConfig)
```

Configuration for a feature flags provider that uses the Split API

<a id="core.project.SplitFeatureFlagSettings.provider"></a>

#### provider

The feature flags provider

<a id="core.project.SplitFeatureFlagSettings.api_key"></a>

#### api\_key

The Split API key. Get it from your user settings

<a id="core.project.NoopFeatureFlagSettings"></a>

## NoopFeatureFlagSettings Objects

```python
class NoopFeatureFlagSettings(BaseFeatureFlagConfig)
```

Configuration for a feature flags provider that does nothing

<a id="core.project.NoopFeatureFlagSettings.provider"></a>

#### provider

The feature flags provider

<a id="core.project.FeatureFlagConfig"></a>

#### FeatureFlagConfig

A union of all feature flag provider configurations

<a id="core.project.Workspace"></a>

## Workspace Objects

```python
class Workspace(_BaseSettings)
```

A workspace is a collection of pipelines, sinks, publishers, scripts, and notebooks in a subdirectory of the project

<a id="core.project.Workspace.workspace_path"></a>

#### workspace\_path

The path to the workspace within the project path

<a id="core.project.Workspace.project_path"></a>

#### project\_path

The path to the project

<a id="core.project.Workspace.name"></a>

#### name

The name of the workspace

<a id="core.project.Workspace.owner"></a>

#### owner

The owner of the workspace

<a id="core.project.Workspace.pipelines"></a>

#### pipelines

Pipelines move data from sources to sinks

<a id="core.project.Workspace.sinks"></a>

#### sinks

A sink is a destination for data

<a id="core.project.Workspace.publishers"></a>

#### publishers

Publishers send data to external systems

<a id="core.project.Workspace.scripts"></a>

#### scripts

Scripts are used to automate tasks

<a id="core.project.Workspace.notebooks"></a>

#### notebooks

Notebooks are used for data analysis and reporting

<a id="core.project.Workspace.path"></a>

#### path

```python
@property
def path() -> Path
```

Get the path to the workspace

<a id="core.project.Workspace.__getitem__"></a>

#### \_\_getitem\_\_

```python
def __getitem__(key: str) -> t.Any
```

Get a component by name

<a id="core.project.Workspace.__setitem__"></a>

#### \_\_setitem\_\_

```python
def __setitem__(key: str, value: t.Any) -> None
```

Set a component by name

<a id="core.project.Workspace.__delitem__"></a>

#### \_\_delitem\_\_

```python
def __delitem__(key: str) -> None
```

Delete a component by name

<a id="core.project.Workspace.__len__"></a>

#### \_\_len\_\_

```python
def __len__() -> int
```

Get the number of components

<a id="core.project.Workspace.__iter__"></a>

#### \_\_iter\_\_

```python
def __iter__() -> t.Iterator[spec.CoreSpecification]
```

Iterate over the components

<a id="core.project.Workspace.__contains__"></a>

#### \_\_contains\_\_

```python
def __contains__(key: str) -> bool
```

Check if a component exists

<a id="core.project.Workspace.get_component_names"></a>

#### get\_component\_names

```python
def get_component_names() -> t.List[str]
```

Get the component names

<a id="core.project.Workspace.items"></a>

#### items

```python
def items() -> t.Iterator[t.Tuple[str, spec.CoreSpecification]]
```

Iterate over the components

<a id="core.project.Workspace.get_pipeline_spec"></a>

#### get\_pipeline\_spec

```python
def get_pipeline_spec(
        name: str) -> M.Result[spec.PipelineSpecification, Exception]
```

Get a pipeline by name

<a id="core.project.Workspace.get_sink_spec"></a>

#### get\_sink\_spec

```python
def get_sink_spec(name: str) -> M.Result[spec.SinkSpecification, Exception]
```

Get a sink by name

<a id="core.project.Workspace.get_publisher_spec"></a>

#### get\_publisher\_spec

```python
def get_publisher_spec(
        name: str) -> M.Result[spec.PublisherSpecification, Exception]
```

Get a publisher by name

<a id="core.project.Workspace.get_script_spec"></a>

#### get\_script\_spec

```python
def get_script_spec(
        name: str) -> M.Result[spec.ScriptSpecification, Exception]
```

Get a script by name

<a id="core.project.Workspace.get_notebook_spec"></a>

#### get\_notebook\_spec

```python
def get_notebook_spec(
        name: str) -> M.Result[spec.NotebookSpecification, Exception]
```

Get a notebook by name

<a id="core.project.Workspace.project"></a>

#### project

```python
@property
def project() -> "Project"
```

Get the project this workspace belongs to

<a id="core.project.Workspace.has_project_association"></a>

#### has\_project\_association

```python
@property
def has_project_association() -> bool
```

Check if the workspace is associated with a project

<a id="core.project.Workspace.inject_configuration"></a>

#### inject\_configuration

```python
@contextmanager
def inject_configuration() -> t.Iterator[None]
```

Inject the workspace configuration into the context

<a id="core.project.Workspace.fs_adapter"></a>

#### fs\_adapter

```python
@property
def fs_adapter() -> FilesystemAdapter
```

Get a handle to the project filesystem adapter

<a id="core.project.Workspace.ff_adapter"></a>

#### ff\_adapter

```python
@property
def ff_adapter() -> AbstractFeatureFlagAdapter
```

Get a handle to the project feature flag adapter

<a id="core.project.Workspace.state"></a>

#### state

```python
@property
def state() -> StateStore
```

Get a handle to the project state store

<a id="core.project.Workspace.get_transform_gateways"></a>

#### get\_transform\_gateways

```python
def get_transform_gateways() -> t.Iterator[t.Tuple[str, "GatewayConfig"]]
```

Get the SQLMesh gateway configurations

<a id="core.project.Workspace.get_transform_context"></a>

#### get\_transform\_context

```python
def get_transform_context(name: t.Optional[str] = None)
```

Get the SQLMesh context for the workspace

We expect a config.py file in the workspace directory that uses the
`get_transform_gateways` method to populate the SQLMesh Config.gateways key.

**Arguments**:

- `name` - The name of the gateway to use.
  

**Returns**:

  The SQLMesh context.

<a id="core.project.Project"></a>

## Project Objects

```python
class Project(_BaseSettings)
```

A project is a collection of workspaces and configuration settings

<a id="core.project.Project.path"></a>

#### path

The path to the project

<a id="core.project.Project.name"></a>

#### name

The name of the project

<a id="core.project.Project.version"></a>

#### version

The version of the project

<a id="core.project.Project.owner"></a>

#### owner

The owner of the project

<a id="core.project.Project.documentation"></a>

#### documentation

The project documentation

<a id="core.project.Project.workspaces"></a>

#### workspaces

The project workspaces

<a id="core.project.Project.fs"></a>

#### fs

The project filesystem settings

<a id="core.project.Project.ff"></a>

#### ff

The project feature flags provider settings

<a id="core.project.Project.state"></a>

#### state

The project state connection settings

<a id="core.project.Project.__getitem__"></a>

#### \_\_getitem\_\_

```python
def __getitem__(key: str) -> t.Any
```

Get an item from the configuration

<a id="core.project.Project.__setitem__"></a>

#### \_\_setitem\_\_

```python
def __setitem__(key: str, value: t.Any) -> None
```

Set an item in the configuration

<a id="core.project.Project.__delitem__"></a>

#### \_\_delitem\_\_

```python
def __delitem__(key: str) -> None
```

Delete a workspace

<a id="core.project.Project.__len__"></a>

#### \_\_len\_\_

```python
def __len__() -> int
```

Get the number of workspaces

<a id="core.project.Project.__iter__"></a>

#### \_\_iter\_\_

```python
def __iter__() -> t.Iterator[Workspace]
```

Iterate over the workspaces

<a id="core.project.Project.__contains__"></a>

#### \_\_contains\_\_

```python
def __contains__(key: str) -> bool
```

Check if a workspace exists

<a id="core.project.Project.get_workspace_names"></a>

#### get\_workspace\_names

```python
def get_workspace_names() -> t.List[str]
```

Get the workspace names

<a id="core.project.Project.items"></a>

#### items

```python
def items() -> t.Iterator[t.Tuple[str, Workspace]]
```

Iterate over the workspaces

<a id="core.project.Project.get_workspace"></a>

#### get\_workspace

```python
def get_workspace(name: str) -> M.Result[Workspace, Exception]
```

Get a workspace by name

<a id="core.project.Project.get_workspace_from_path"></a>

#### get\_workspace\_from\_path

```python
def get_workspace_from_path(path: PathLike) -> M.Result[Workspace, Exception]
```

Get a workspace by path.

<a id="core.project.Project.to_scoped_dict"></a>

#### to\_scoped\_dict

```python
def to_scoped_dict(workspace: t.Optional[str] = None) -> ChainMap[str, t.Any]
```

Convert the project settings to a scoped dictionary

Lookups are performed in the following order:
- The extra configuration, holding data set via __setitem__.
- The workspace configuration, if passed.
- The project configuration.
- The wrapped configuration, if available. Typically a dynaconf settings object.

Boxing allows us to access nested values using dot notation. This is doubly useful
since ChainMaps will move to the next map in the chain if the dotted key is not
fully resolved in the current map.

<a id="core.project.Project.inject_configuration"></a>

#### inject\_configuration

```python
@contextmanager
def inject_configuration(
        workspace: t.Optional[str] = None) -> t.Iterator[None]
```

Inject the project configuration into the context

<a id="core.project.Project.fs_adapter"></a>

#### fs\_adapter

```python
@cached_property
def fs_adapter() -> FilesystemAdapter
```

Get a configured filesystem adapter

<a id="core.project.Project.ff_adapter"></a>

#### ff\_adapter

```python
@cached_property
def ff_adapter() -> AbstractFeatureFlagAdapter
```

Get a handle to the project's configured feature flag adapter

<a id="core.project.Project.duckdb"></a>

#### duckdb

```python
@cached_property
def duckdb() -> duckdb.DuckDBPyConnection
```

Get a handle to the project's DuckDB connection

<a id="core.project.Project.get_workspace_path"></a>

#### get\_workspace\_path

```python
def get_workspace_path(name: str) -> M.Result[Path, Exception]
```

Get the path to a workspace by name

<a id="core.project.Project.from_path"></a>

#### from\_path

```python
@classmethod
def from_path(cls, root: PathLike)
```

Load configuration data from a project root path using dynaconf.

**Arguments**:

- `root` - The root path to the project.
  

**Returns**:

  A Project object.

<a id="core.project.Project.activate"></a>

#### activate

```python
def activate() -> t.Callable[[], None]
```

Activate the project and return a deactivation function

<a id="core.project.Project.activated"></a>

#### activated

```python
@contextmanager
def activated() -> t.Iterator[None]
```

Activate the project for the duration of the context

<a id="core.project.load_project"></a>

#### load\_project

Load configuration data from a project root path using dynaconf.

**Arguments**:

- `root` - The root path to the project.
  

**Returns**:

  A Result monad with a Project object if successful. Otherwise, a Result monad with an error.

<a id="core.state"></a>

# core.state

The state module is responible for providing an adapter through which we can persist data

<a id="core.state.StateStore"></a>

## StateStore Objects

```python
class StateStore(pydantic.BaseModel)
```

The state store is responsible for persisting data

<a id="core.state.StateStore.schema"></a>

#### schema

The schema in which to store data

<a id="core.state.StateStore.protected"></a>

#### protected

Whether the state store is protected, i.e. should never be torn down

A safety measure to prevent accidental data loss when users are consuming the cdf API
directly. This should be set to False when running tests or you know what you're doing.

<a id="core.state.StateStore.connection"></a>

#### connection

The connection configuration to the state store

<a id="core.state.StateStore.adapter"></a>

#### adapter

```python
@property
def adapter() -> EngineAdapter
```

The adapter to the state store

<a id="core.state.StateStore.setup"></a>

#### setup

```python
def setup() -> None
```

Setup the state store

<a id="core.state.StateStore.teardown"></a>

#### teardown

```python
def teardown() -> None
```

Teardown the state store

<a id="core.state.StateStore.store_json"></a>

#### store\_json

```python
def store_json(key: str, value: t.Any) -> None
```

Store a JSON value

<a id="core.state.StateStore.__del__"></a>

#### \_\_del\_\_

```python
def __del__() -> None
```

Close the connection to the state store

<a id="core.utility"></a>

# core.utility

<a id="core.utility.find_item"></a>

#### find\_item

```python
def find_item(lst: t.List[TDict], key: t.Union[t.Callable[[TDict], t.Any],
                                               str], value: t.Any) -> TDict
```

Find an item in a list by a key-value pair.

**Example**:

  >>> find_item([{"name": "Alice"}, {"name": "Bob"}], "name", "Bob")
- `{"name"` - "Bob"}
  

**Arguments**:

- `lst` - The list to search.
- `key` - The key function to extract the value from an item or the key name.
- `value` - The value to find.
  

**Returns**:

  The item with the matching value.

<a id="core.utility.file"></a>

# core.utility.file

<a id="core.utility.file.load_file"></a>

#### load\_file

```python
def load_file(path: Path) -> M.Result[t.Dict[str, t.Any], Exception]
```

Load a configuration from a file path.

**Arguments**:

- `path` - The file path.
  

**Returns**:

  A Result monad with the configuration dictionary if the file format is JSON, YAML or TOML.
  Otherwise, a Result monad with an error.

<a id="core.feature_flag.launchdarkly"></a>

# core.feature\_flag.launchdarkly

LaunchDarkly feature flag provider.

<a id="core.feature_flag.launchdarkly.LaunchDarklyFeatureFlagAdapter"></a>

## LaunchDarklyFeatureFlagAdapter Objects

```python
class LaunchDarklyFeatureFlagAdapter(AbstractFeatureFlagAdapter)
```

A feature flag adapter that uses LaunchDarkly.

<a id="core.feature_flag.launchdarkly.LaunchDarklyFeatureFlagAdapter.__init__"></a>

#### \_\_init\_\_

```python
@with_config(sections=("feature_flags", ))
def __init__(sdk_key: str, **kwargs: t.Any) -> None
```

Initialize the LaunchDarkly feature flags.

**Arguments**:

- `sdk_key` - The SDK key to use for LaunchDarkly.

<a id="core.feature_flag.harness"></a>

# core.feature\_flag.harness

Harness feature flag provider.

<a id="core.feature_flag.harness.HarnessFeatureFlagAdapter"></a>

## HarnessFeatureFlagAdapter Objects

```python
class HarnessFeatureFlagAdapter(AbstractFeatureFlagAdapter)
```

<a id="core.feature_flag.harness.HarnessFeatureFlagAdapter.__init__"></a>

#### \_\_init\_\_

```python
@with_config(sections=("feature_flags", ))
def __init__(sdk_key: str = dlt.secrets.value,
             api_key: str = dlt.secrets.value,
             account: str = dlt.secrets.value,
             organization: str = dlt.secrets.value,
             project: str = dlt.secrets.value,
             **kwargs: t.Any) -> None
```

Initialize the adapter.

<a id="core.feature_flag.harness.HarnessFeatureFlagAdapter.client"></a>

#### client

```python
@property
def client() -> CfClient
```

Get the client and cache it in the instance.

<a id="core.feature_flag.harness.HarnessFeatureFlagAdapter.pool"></a>

#### pool

```python
@property
def pool() -> ThreadPoolExecutor
```

Get the thread pool.

<a id="core.feature_flag.harness.HarnessFeatureFlagAdapter.get"></a>

#### get

```python
def get(feature_name: str) -> FlagAdapterResponse
```

Get a feature flag.

<a id="core.feature_flag.harness.HarnessFeatureFlagAdapter.get_all_feature_names"></a>

#### get\_all\_feature\_names

```python
def get_all_feature_names() -> t.List[str]
```

Get all the feature flags.

<a id="core.feature_flag.harness.HarnessFeatureFlagAdapter.save"></a>

#### save

```python
def save(feature_name: str, flag: bool) -> None
```

Create a feature flag.

<a id="core.feature_flag.harness.HarnessFeatureFlagAdapter.save_many"></a>

#### save\_many

```python
def save_many(flags: t.Dict[str, bool]) -> None
```

Create many feature flags.

<a id="core.feature_flag.harness.HarnessFeatureFlagAdapter.delete"></a>

#### delete

```python
def delete(feature_name: str) -> None
```

Drop a feature flag.

<a id="core.feature_flag.harness.HarnessFeatureFlagAdapter.delete_many"></a>

#### delete\_many

```python
def delete_many(feature_names: t.List[str]) -> None
```

Drop many feature flags.

<a id="core.feature_flag.harness.HarnessFeatureFlagAdapter.apply_source"></a>

#### apply\_source

```python
def apply_source(source: DltSource, *namespace: str) -> DltSource
```

Apply the feature flags to a dlt source.

<a id="core.feature_flag.harness.HarnessFeatureFlagAdapter.__del__"></a>

#### \_\_del\_\_

```python
def __del__() -> None
```

Close the client.

<a id="core.feature_flag"></a>

# core.feature\_flag

Feature flag providers implement a uniform interface and are wrapped by an adapter.

The adapter is responsible for loading the correct provider and applying the feature flags within
various contexts in cdf. This allows for a clean separation of concerns and makes it easy to
implement new feature flag providers in the future.

<a id="core.feature_flag.ADAPTERS"></a>

#### ADAPTERS

Feature flag provider adapters classes by name.

<a id="core.feature_flag.get_feature_flag_adapter_cls"></a>

#### get\_feature\_flag\_adapter\_cls

```python
@with_config(sections=("feature_flags", ))
def get_feature_flag_adapter_cls(
    provider: str = dlt.config.value
) -> M.Result[t.Type[AbstractFeatureFlagAdapter], Exception]
```

Get a feature flag adapter by name.

**Arguments**:

- `provider` - The name of the feature flag adapter.
- `options` - The configuration for the feature flag adapter.
  

**Returns**:

  The feature flag adapter.

<a id="core.feature_flag.file"></a>

# core.feature\_flag.file

File-based feature flag provider.

<a id="core.feature_flag.file.FilesystemFeatureFlagAdapter"></a>

## FilesystemFeatureFlagAdapter Objects

```python
class FilesystemFeatureFlagAdapter(AbstractFeatureFlagAdapter)
```

A feature flag adapter that uses the filesystem.

<a id="core.feature_flag.file.FilesystemFeatureFlagAdapter.__init__"></a>

#### \_\_init\_\_

```python
@with_config(sections=("feature_flags", ))
def __init__(filesystem: fsspec.AbstractFileSystem,
             filename: str = dlt.config.value,
             **kwargs: t.Any) -> None
```

Initialize the filesystem feature flags.

**Arguments**:

- `filesystem` - The filesystem to use.
- `filename` - The filename to use for the feature flags.

<a id="core.feature_flag.file.FilesystemFeatureFlagAdapter.get"></a>

#### get

```python
def get(feature_name: str) -> FlagAdapterResponse
```

Get a feature flag.

**Arguments**:

- `feature_name` - The name of the feature flag.
  

**Returns**:

  The feature flag.

<a id="core.feature_flag.file.FilesystemFeatureFlagAdapter.get_all_feature_names"></a>

#### get\_all\_feature\_names

```python
def get_all_feature_names() -> t.List[str]
```

Get all feature flag names.

**Returns**:

  The feature flag names.

<a id="core.feature_flag.file.FilesystemFeatureFlagAdapter.save"></a>

#### save

```python
def save(feature_name: str, flag: bool) -> None
```

Save a feature flag.

**Arguments**:

- `feature_name` - The name of the feature flag.
- `flag` - The value of the feature flag.

<a id="core.feature_flag.file.FilesystemFeatureFlagAdapter.save_many"></a>

#### save\_many

```python
def save_many(flags: t.Dict[str, bool]) -> None
```

Save multiple feature flags.

**Arguments**:

- `flags` - The feature flags to save.

<a id="core.feature_flag.split"></a>

# core.feature\_flag.split

Split feature flag provider.

<a id="core.feature_flag.split.SplitFeatureFlagAdapter"></a>

## SplitFeatureFlagAdapter Objects

```python
class SplitFeatureFlagAdapter(AbstractFeatureFlagAdapter)
```

A feature flag adapter that uses Split.

<a id="core.feature_flag.split.SplitFeatureFlagAdapter.__init__"></a>

#### \_\_init\_\_

```python
def __init__(sdk_key: str, **kwargs: t.Any) -> None
```

Initialize the Split feature flags.

**Arguments**:

- `sdk_key` - The SDK key to use for Split.

<a id="core.feature_flag.noop"></a>

# core.feature\_flag.noop

No-op feature flag provider.

<a id="core.feature_flag.noop.NoopFeatureFlagAdapter"></a>

## NoopFeatureFlagAdapter Objects

```python
class NoopFeatureFlagAdapter(AbstractFeatureFlagAdapter)
```

A feature flag adapter that does nothing.

<a id="core.feature_flag.noop.NoopFeatureFlagAdapter.__init__"></a>

#### \_\_init\_\_

```python
def __init__(**kwargs: t.Any) -> None
```

Initialize the adapter.

<a id="core.feature_flag.base"></a>

# core.feature\_flag.base

<a id="core.feature_flag.base.FlagAdapterResponse"></a>

## FlagAdapterResponse Objects

```python
class FlagAdapterResponse(Enum)
```

Feature flag response.

This enum is used to represent the state of a feature flag. It is similar
to a boolean but with an extra state for when the flag is not found.

<a id="core.feature_flag.base.FlagAdapterResponse.ENABLED"></a>

#### ENABLED

The feature flag is enabled.

<a id="core.feature_flag.base.FlagAdapterResponse.DISABLED"></a>

#### DISABLED

The feature flag is disabled.

<a id="core.feature_flag.base.FlagAdapterResponse.NOT_FOUND"></a>

#### NOT\_FOUND

The feature flag is not found.

<a id="core.feature_flag.base.FlagAdapterResponse.__bool__"></a>

#### \_\_bool\_\_

```python
def __bool__() -> bool
```

Return True if the flag is enabled and False otherwise.

<a id="core.feature_flag.base.FlagAdapterResponse.__eq__"></a>

#### \_\_eq\_\_

```python
def __eq__(value: object) -> bool
```

Compare the flag to a boolean.

<a id="core.feature_flag.base.FlagAdapterResponse.from_bool"></a>

#### from\_bool

```python
@classmethod
def from_bool(cls, flag: bool) -> "FlagAdapterResponse"
```

Convert a boolean to a flag response.

<a id="core.feature_flag.base.AbstractFeatureFlagAdapter"></a>

## AbstractFeatureFlagAdapter Objects

```python
class AbstractFeatureFlagAdapter(abc.ABC)
```

Abstract feature flag adapter.

<a id="core.feature_flag.base.AbstractFeatureFlagAdapter.__init__"></a>

#### \_\_init\_\_

```python
def __init__(**kwargs: t.Any) -> None
```

Initialize the adapter.

<a id="core.feature_flag.base.AbstractFeatureFlagAdapter.get"></a>

#### get

```python
@abc.abstractmethod
def get(feature_name: str) -> FlagAdapterResponse
```

Get the feature flag.

<a id="core.feature_flag.base.AbstractFeatureFlagAdapter.__getitem__"></a>

#### \_\_getitem\_\_

```python
def __getitem__(feature_name: str) -> FlagAdapterResponse
```

Get the feature flag.

<a id="core.feature_flag.base.AbstractFeatureFlagAdapter.get_many"></a>

#### get\_many

```python
def get_many(feature_names: t.List[str]) -> t.Dict[str, FlagAdapterResponse]
```

Get many feature flags.

Implementations should override this method if they can optimize it. The default
will call get in a loop.

<a id="core.feature_flag.base.AbstractFeatureFlagAdapter.save"></a>

#### save

```python
@abc.abstractmethod
def save(feature_name: str, flag: bool) -> None
```

Save the feature flag.

<a id="core.feature_flag.base.AbstractFeatureFlagAdapter.__setitem__"></a>

#### \_\_setitem\_\_

```python
def __setitem__(feature_name: str, flag: bool) -> None
```

Save the feature flag.

<a id="core.feature_flag.base.AbstractFeatureFlagAdapter.save_many"></a>

#### save\_many

```python
def save_many(flags: t.Dict[str, bool]) -> None
```

Save many feature flags.

Implementations should override this method if they can optimize it. The default
will call save in a loop.

<a id="core.feature_flag.base.AbstractFeatureFlagAdapter.get_all_feature_names"></a>

#### get\_all\_feature\_names

```python
@abc.abstractmethod
def get_all_feature_names() -> t.List[str]
```

Get all feature names.

<a id="core.feature_flag.base.AbstractFeatureFlagAdapter.keys"></a>

#### keys

```python
def keys() -> t.List[str]
```

Get all feature names.

<a id="core.feature_flag.base.AbstractFeatureFlagAdapter.__iter__"></a>

#### \_\_iter\_\_

```python
def __iter__() -> t.Iterator[str]
```

Iterate over the feature names.

<a id="core.feature_flag.base.AbstractFeatureFlagAdapter.__contains__"></a>

#### \_\_contains\_\_

```python
def __contains__(feature_name: str) -> bool
```

Check if a feature flag exists.

<a id="core.feature_flag.base.AbstractFeatureFlagAdapter.__len__"></a>

#### \_\_len\_\_

```python
def __len__() -> int
```

Get the number of feature flags.

<a id="core.feature_flag.base.AbstractFeatureFlagAdapter.delete"></a>

#### delete

```python
def delete(feature_name: str) -> None
```

Delete a feature flag.

By default, this will disable the flag but implementations can override this method
to delete the flag.

<a id="core.feature_flag.base.AbstractFeatureFlagAdapter.delete_many"></a>

#### delete\_many

```python
def delete_many(feature_names: t.List[str]) -> None
```

Delete many feature flags.

<a id="core.feature_flag.base.AbstractFeatureFlagAdapter.apply_source"></a>

#### apply\_source

```python
def apply_source(source: "DltSource", *namespace: str) -> "DltSource"
```

Apply the feature flags to a dlt source.

**Arguments**:

- `source` - The source to apply the feature flags to.
  

**Returns**:

  The source with the feature flags applied.

<a id="integrations"></a>

# integrations

<a id="integrations.slack"></a>

# integrations.slack

<a id="integrations.slack.SlackMessageComposer"></a>

## SlackMessageComposer Objects

```python
class SlackMessageComposer()
```

Builds Slack message with primary and secondary blocks

<a id="integrations.slack.SlackMessageComposer.__init__"></a>

#### \_\_init\_\_

```python
def __init__(initial_message: t.Optional[TSlackMessage] = None) -> None
```

Initialize the Slack message builder

<a id="integrations.slack.SlackMessageComposer.add_primary_blocks"></a>

#### add\_primary\_blocks

```python
def add_primary_blocks(*blocks: TSlackBlock) -> "SlackMessageComposer"
```

Add blocks to the message. Blocks are always displayed

<a id="integrations.slack.SlackMessageComposer.add_secondary_blocks"></a>

#### add\_secondary\_blocks

```python
def add_secondary_blocks(*blocks: TSlackBlock) -> "SlackMessageComposer"
```

Add attachments to the message

Attachments are hidden behind "show more" button. The first 5 attachments
are always displayed. NOTICE: attachments blocks are deprecated by Slack

<a id="integrations.slack.normalize_message"></a>

#### normalize\_message

```python
def normalize_message(
        message: t.Union[str, t.List[str], t.Iterable[str]]) -> str
```

Normalize message to fit Slack's max text length

<a id="integrations.slack.divider_block"></a>

#### divider\_block

```python
def divider_block() -> dict
```

Create a divider block

<a id="integrations.slack.fields_section_block"></a>

#### fields\_section\_block

```python
def fields_section_block(*messages: str) -> dict
```

Create a section block with multiple fields

<a id="integrations.slack.text_section_block"></a>

#### text\_section\_block

```python
def text_section_block(message: str) -> dict
```

Create a section block with text

<a id="integrations.slack.empty_section_block"></a>

#### empty\_section\_block

```python
def empty_section_block() -> dict
```

Create an empty section block

<a id="integrations.slack.context_block"></a>

#### context\_block

```python
def context_block(*messages: str) -> dict
```

Create a context block with multiple fields

<a id="integrations.slack.header_block"></a>

#### header\_block

```python
def header_block(message: str) -> dict
```

Create a header block

<a id="integrations.slack.button_action_block"></a>

#### button\_action\_block

```python
def button_action_block(text: str, url: str) -> dict
```

Create a button action block

<a id="integrations.slack.compacted_sections_blocks"></a>

#### compacted\_sections\_blocks

```python
def compacted_sections_blocks(
        *messages: t.Union[str, t.Iterable[str]]) -> t.List[dict]
```

Create a list of compacted sections blocks

<a id="integrations.slack.SlackAlertIcon"></a>

## SlackAlertIcon Objects

```python
class SlackAlertIcon(str, Enum)
```

Enum for status of the alert

<a id="integrations.slack.stringify_list"></a>

#### stringify\_list

```python
def stringify_list(list_variation: t.Union[t.List[str], str]) -> str
```

Prettify and deduplicate list of strings converting it to a newline delimited string

<a id="integrations.slack.send_basic_slack_message"></a>

#### send\_basic\_slack\_message

```python
def send_basic_slack_message(incoming_hook: str,
                             message: str,
                             is_markdown: bool = True) -> None
```

Sends a `message` to  Slack `incoming_hook`, by default formatted as markdown.

<a id="integrations.slack.send_extract_start_slack_message"></a>

#### send\_extract\_start\_slack\_message

```python
def send_extract_start_slack_message(incoming_hook: str, source: str,
                                     run_id: str, tags: t.List[str],
                                     owners: t.List[str], environment: str,
                                     resources_selected: t.List[str],
                                     resources_count: int) -> None
```

Sends a Slack message for the start of an extract

<a id="integrations.slack.send_extract_failure_message"></a>

#### send\_extract\_failure\_message

```python
def send_extract_failure_message(incoming_hook: str, source: str, run_id: str,
                                 duration: float, error: Exception) -> None
```

Sends a Slack message for the failure of an extract

<a id="integrations.slack.send_extract_success_message"></a>

#### send\_extract\_success\_message

```python
def send_extract_success_message(incoming_hook: str, source: str, run_id: str,
                                 duration: float) -> None
```

Sends a Slack message for the success of an extract

<a id="integrations.slack.send_normalize_start_slack_message"></a>

#### send\_normalize\_start\_slack\_message

```python
def send_normalize_start_slack_message(incoming_hook: str, source: str,
                                       blob_name: str, run_id: str,
                                       environment: str) -> None
```

Sends a Slack message for the start of an extract

<a id="integrations.slack.send_normalize_failure_message"></a>

#### send\_normalize\_failure\_message

```python
def send_normalize_failure_message(incoming_hook: str, source: str,
                                   blob_name: str, run_id: str,
                                   duration: float, error: Exception) -> None
```

Sends a Slack message for the failure of an normalization

<a id="integrations.slack.send_normalization_success_message"></a>

#### send\_normalization\_success\_message

```python
def send_normalization_success_message(incoming_hook: str, source: str,
                                       blob_name: str, run_id: str,
                                       duration: float) -> None
```

Sends a Slack message for the success of an normalization

<a id="integrations.slack.send_load_start_slack_message"></a>

#### send\_load\_start\_slack\_message

```python
def send_load_start_slack_message(incoming_hook: str, source: str,
                                  destination: str, dataset: str,
                                  run_id: str) -> None
```

Sends a Slack message for the start of a load

<a id="integrations.slack.send_load_failure_message"></a>

#### send\_load\_failure\_message

```python
def send_load_failure_message(incoming_hook: str, source: str,
                              destination: str, dataset: str,
                              run_id: str) -> None
```

Sends a Slack message for the failure of an load

<a id="integrations.slack.send_load_success_message"></a>

#### send\_load\_success\_message

```python
def send_load_success_message(incoming_hook: str, source: str,
                              destination: str, dataset: str, run_id: str,
                              payload: str) -> None
```

Sends a Slack message for the success of an normalization

