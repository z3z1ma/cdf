<a id="local"></a>

# local

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
def is_main(name: t.Optional[str] = None) -> bool
```

Check if the current module is being run as the main program in cdf context.

Also injects a hook in debug mode to allow dropping into user code via pdb.

**Arguments**:

- `name` _str, optional_ - The name of the module to check. If None, the calling module is
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
                                                help="Environment to use.")
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
def index(ctx: typer.Context) -> None
```

:page_with_curl: Print an index of [b][blue]Pipelines[/blue], [red]Models[/red], [yellow]Publishers[/yellow][/b], and other components.

<a id="cli.pipeline"></a>

#### pipeline

```python
@app.command(rich_help_panel="Data Management")
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

:inbox_tray: Ingest data from a [b blue]pipeline[/b blue] into a data store where it can be [b red]Transformed[/b red].


**Arguments**:

- `ctx` - The CLI context.
- `pipeline_to_sink` - The pipeline and sink separated by a colon.
- `select` - The resources to ingest as a sequence of glob patterns.
- `exclude` - The resources to exclude as a sequence of glob patterns.
- `force_replace` - Whether to force replace the write disposition.
- `no_stage` - Whether to disable staging the data in the sink.

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

:mag: Evaluates a :zzz: Lazy [b blue]pipeline[/b blue] and enumerates the discovered resources.


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
@app.command(rich_help_panel="Data Management")
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

<a id="cli.execute_script"></a>

#### execute\_script

```python
@app.command("execute-script", rich_help_panel="Utilities")
def execute_script(
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

<a id="cli.execute_notebook"></a>

#### execute\_notebook

```python
@app.command("execute-notebook", rich_help_panel="Utilities")
def execute_notebook(
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
    "jupyter-lab",
    rich_help_panel="Utilities",
    context_settings={
        "allow_extra_args": True,
        "ignore_unknown_options": True
    },
)
def jupyter_lab(ctx: typer.Context) -> None
```

:notebook: Start a Jupyter Lab server.

<a id="cli.spec"></a>

#### spec

```python
@app.command(rich_help_panel="Develop")
def spec(name: _SpecType, json_schema: bool = False) -> None
```

:mag: Print the fields for a given spec type.


**Arguments**:

- `name` - The name of the spec to print.
- `json_schema` - Whether to print the JSON schema for the spec.

<a id="types"></a>

# types

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
def pure(cls, value: K) -> "Result[K, Exception]"
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

<a id="core.configuration"></a>

# core.configuration

CDF configuration

Config can be defined as a dictionary or a file path. If a file path is given, then it must be
either JSON, YAML or TOML format.

<a id="core.configuration.ParsedConfiguration"></a>

## ParsedConfiguration Objects

```python
class ParsedConfiguration(t.TypedDict)
```

A container for configuration data

<a id="core.configuration.load_config"></a>

#### load\_config

```python
@M.result
def load_config(root: PathLike) -> ParsedConfiguration
```

Load configuration data from a project root path.

**Arguments**:

- `root` - The root path to the project.
  

**Returns**:

  A Result monad with the configuration data if successful. Otherwise, a Result monad with an
  error.

<a id="core.filesystem"></a>

# core.filesystem

An adapter interface for filesystems.

<a id="core.filesystem.load_filesystem_provider"></a>

#### load\_filesystem\_provider

```python
@with_config(sections=("filesystem", ))
def load_filesystem_provider(
    provider: t.Optional[str] = None,
    root: t.Optional[PathLike] = None,
    options: t.Optional[t.Dict[str,
                               t.Any]] = None) -> fsspec.AbstractFileSystem
```

Load a filesystem from a provider and kwargs.

**Arguments**:

- `provider` - The filesystem provider.
- `root` - The root path for the filesystem.
- `options` - The filesystem provider kwargs.
  

**Returns**:

  The filesystem.

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

<a id="core.runtime"></a>

# core.runtime

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

<a id="core.runtime.pipeline.RuntimeContext"></a>

## RuntimeContext Objects

```python
class RuntimeContext(t.NamedTuple)
```

The runtime context for a pipeline.

<a id="core.runtime.pipeline.RuntimeContext.pipeline_name"></a>

#### pipeline\_name

The pipeline name.

<a id="core.runtime.pipeline.RuntimeContext.dataset_name"></a>

#### dataset\_name

The dataset name.

<a id="core.runtime.pipeline.RuntimeContext.destination"></a>

#### destination

The destination.

<a id="core.runtime.pipeline.RuntimeContext.staging"></a>

#### staging

The staging location.

<a id="core.runtime.pipeline.RuntimeContext.select"></a>

#### select

A list of glob patterns to select resources.

<a id="core.runtime.pipeline.RuntimeContext.exclude"></a>

#### exclude

A list of glob patterns to exclude resources.

<a id="core.runtime.pipeline.RuntimeContext.force_replace"></a>

#### force\_replace

Whether to force replace disposition.

<a id="core.runtime.pipeline.RuntimeContext.intercept_sources"></a>

#### intercept\_sources

Stores the intercepted sources in itself if provided.

<a id="core.runtime.pipeline.RuntimeContext.enable_stage"></a>

#### enable\_stage

Whether to stage data if a staging location is provided.

<a id="core.runtime.pipeline.RuntimeContext.applicator"></a>

#### applicator

The transformation to apply to the sources.

<a id="core.runtime.pipeline.RuntimeContext.metrics"></a>

#### metrics

A container for captured metrics during extract.

<a id="core.runtime.pipeline.runtime_context"></a>

#### runtime\_context

```python
@contextmanager
def runtime_context(
        pipeline_name: str,
        dataset_name: str,
        destination: TDestinationReferenceArg,
        staging: t.Optional[TDestinationReferenceArg] = None,
        select: t.Optional[t.List[str]] = None,
        exclude: t.Optional[t.List[str]] = None,
        force_replace: bool = False,
        intercept_sources: t.Optional[t.Set[dlt.sources.DltSource]] = None,
        enable_stage: bool = True,
        applicator: t.Callable[[dlt.sources.DltSource],
                               dlt.sources.DltSource] = _ident,
        metrics: t.Optional[t.Mapping[str, t.Any]] = None) -> t.Iterator[None]
```

A context manager for setting the runtime context.

This allows the cdf library to set the context prior to running the pipeline which is
ultimately evaluating user code.

<a id="core.runtime.pipeline.RuntimePipeline"></a>

## RuntimePipeline Objects

```python
class RuntimePipeline(Pipeline)
```

Overrides certain methods of the dlt pipeline to allow for cdf specific behavior.

<a id="core.runtime.pipeline.pipeline_factory"></a>

#### pipeline\_factory

```python
def pipeline_factory() -> RuntimePipeline
```

Creates a cdf pipeline. This is used in lieu of dlt.pipeline. in user code.

A cdf pipeline is a wrapper around a dlt pipeline that leverages injected information
from the runtime context. Raises a ValueError if the runtime context is not set.

<a id="core.runtime.pipeline.execute_pipeline_specification"></a>

#### execute\_pipeline\_specification

```python
def execute_pipeline_specification(
    spec: PipelineSpecification,
    destination: TDestinationReferenceArg,
    staging: t.Optional[TDestinationReferenceArg] = None,
    select: t.Optional[t.List[str]] = None,
    exclude: t.Optional[t.List[str]] = None,
    force_replace: bool = False,
    intercept_sources: bool = False,
    enable_stage: bool = True,
    quiet: bool = False
) -> t.Union[
        M.Result[t.Dict[str, t.Any], Exception],
        M.Result[t.Set[dlt.sources.DltSource], Exception],
]
```

Executes a pipeline specification.

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
def execute_notebook_specification(
        spec: NotebookSpecification,
        storage: t.Optional[fsspec.AbstractFileSystem] = None,
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

<a id="core.logger.__getattr__"></a>

#### \_\_getattr\_\_

```python
def __getattr__(name: str) -> "LogMethod"
```

Get a logger method from the package logger.

<a id="core.logger.monkeypatch_dlt"></a>

#### monkeypatch\_dlt

```python
def monkeypatch_dlt() -> None
```

Monkeypatch the dlt logging module.

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

<a id="core.specification"></a>

# core.specification

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
interface. If the metric is already a metric interface, this should be left empty.

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
def __call__(resource: dlt.sources.DltResource, state: MetricState) -> None
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

<a id="core.specification.pipeline.PipelineSpecification.runtime_metrics"></a>

#### runtime\_metrics

```python
@property
def runtime_metrics() -> types.MappingProxyType[str, t.Dict[str, Metric]]
```

Get a read only view of the runtime metrics.

<a id="core.specification.pipeline.PipelineSpecification.apply"></a>

#### apply

```python
def apply(source: dlt.sources.DltSource) -> dlt.sources.DltSource
```

Apply metrics and filters to a source.

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

Setting this implies the output should be stored. Storage uses the configured fs provider.

This is a format string which will be formatted with the following variables:
- name: The name of the notebook.
- date: The current date.
- timestamp: An ISO formatted timestamp.
- epoch: The current epoch time.
- params: A dict of the resolved parameters passed to the notebook.

<a id="core.specification.notebook.NotebookSpecification.parameters"></a>

#### parameters

Parameters to pass to the notebook when running.

<a id="core.specification.notebook.NotebookSpecification.keep_local_rendered"></a>

#### keep\_local\_rendered

Whether to keep the rendered notebook locally after running.

Rendered notebooks are written to the `_rendered` folder of the notebook's parent directory.
Setting this to False will delete the rendered notebook after running. This is independent
of the long term storage offered by `storage_path` configuration.

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

The name of the component.

<a id="core.specification.base.BaseComponent.version"></a>

#### version

The version of the component.

<a id="core.specification.base.BaseComponent.owner"></a>

#### owner

The owners of the component.

<a id="core.specification.base.BaseComponent.description"></a>

#### description

The description of the component.

<a id="core.specification.base.BaseComponent.tags"></a>

#### tags

Tags for this component used for component queries and integrations.

<a id="core.specification.base.BaseComponent.enabled"></a>

#### enabled

Whether this component is enabled.

<a id="core.specification.base.BaseComponent.meta"></a>

#### meta

Arbitrary user-defined metadata for this component.

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

<a id="core.specification.base.WorkspaceComponent"></a>

## WorkspaceComponent Objects

```python
class WorkspaceComponent(BaseComponent)
```

A component within a workspace.

<a id="core.specification.base.WorkspaceComponent.workspace_path"></a>

#### workspace\_path

The path to the workspace containing the component.

<a id="core.specification.base.WorkspaceComponent.component_path"></a>

#### component\_path

The path to the component within the workspace folder.

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

<a id="core.specification.base.Schedulable.cron"></a>

#### cron

```python
@property
def cron() -> croniter | None
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

The entrypoint of the component.

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

The active project context variable.

<a id="core.context.debug_mode"></a>

#### debug\_mode

The debug mode context variable.

<a id="core.context.CDFConfigProvider"></a>

## CDFConfigProvider Objects

```python
class CDFConfigProvider(ConfigProvider)
```

A configuration provider for CDF settings.

<a id="core.context.inject_cdf_config_provider"></a>

#### inject\_cdf\_config\_provider

```python
def inject_cdf_config_provider(cdf: "ContinuousDataFramework") -> None
```

Injects CDFConfigProvider into the ConfigProvidersContext.

**Arguments**:

- `config` - The configuration to inject

<a id="core.packaging"></a>

# core.packaging

Packaging adapter with PEX implementation.

<a id="core.project"></a>

# core.project

A wrapper around a CDF project.

<a id="core.project.ConfigurationOverlay"></a>

## ConfigurationOverlay Objects

```python
class ConfigurationOverlay(ChainMap[str, t.Any])
```

A ChainMap with attribute access designed to wrap dynaconf settings.

<a id="core.project.ConfigurationOverlay.normalize_script"></a>

#### normalize\_script

```python
@staticmethod
def normalize_script(
    config: t.MutableMapping[str, t.Any],
    type_: str,
    ext: t.Tuple[str, ...] = ("py", )) -> t.MutableMapping[str, t.Any]
```

Normalize a script based configuration.

The name may be a relative path to the script such as sales/mrr.py in which case the
path is kept as-is and the name is normalized to sales_mrr.

Alternatively it could be a name such as mrr in which case the name will be kept as-is
and the component path will be set to mrr_{type_}.py

The final example is a name which is a pathlike without an extension such as sales/mrr in which
case the name will be set to sales_mrr and the path will be set to sales/mrr_{type_}.py

In the event of multiple extensions for a given script type, and the name ommitting the
extension, the first extension is used. Any special characters outside os.sep and a file extension
will cause a pydanitc validation error and prompt the user to update the name property.

<a id="core.project.ContinuousDataFramework"></a>

## ContinuousDataFramework Objects

```python
class ContinuousDataFramework()
```

Common properties shared by Project and Workspace.

<a id="core.project.ContinuousDataFramework.feature_flag_provider"></a>

#### feature\_flag\_provider

```python
@cached_property
def feature_flag_provider() -> FlagProvider
```

The feature flag provider.

<a id="core.project.ContinuousDataFramework.filesystem"></a>

#### filesystem

```python
@cached_property
def filesystem() -> fsspec.AbstractFileSystem
```

The filesystem provider.

<a id="core.project.ContinuousDataFramework.pipelines"></a>

#### pipelines

```python
@cached_property
def pipelines() -> t.Dict[str, PipelineSpecification]
```

Map of pipelines by name.

<a id="core.project.ContinuousDataFramework.sinks"></a>

#### sinks

```python
@cached_property
def sinks() -> t.Dict[str, SinkSpecification]
```

Map of sinks by name.

<a id="core.project.ContinuousDataFramework.publishers"></a>

#### publishers

```python
@cached_property
def publishers() -> t.Dict[str, PublisherSpecification]
```

Map of publishers by name.

<a id="core.project.ContinuousDataFramework.scripts"></a>

#### scripts

```python
@cached_property
def scripts() -> t.Dict[str, ScriptSpecification]
```

Map of scripts by name.

<a id="core.project.ContinuousDataFramework.notebooks"></a>

#### notebooks

```python
@cached_property
def notebooks() -> t.Dict[str, NotebookSpecification]
```

Map of notebooks by name.

<a id="core.project.ContinuousDataFramework.models"></a>

#### models

```python
@cached_property
def models() -> t.Dict[str, sqlmesh.Model]
```

Map of models by name. Uses the default gateway.

<a id="core.project.ContinuousDataFramework.get_pipeline"></a>

#### get\_pipeline

```python
def get_pipeline(name: str) -> M.Result[PipelineSpecification, Exception]
```

Get a pipeline by name.

<a id="core.project.ContinuousDataFramework.get_sink"></a>

#### get\_sink

```python
def get_sink(name: str) -> M.Result[SinkSpecification, Exception]
```

Get a sink by name.

<a id="core.project.ContinuousDataFramework.get_publisher"></a>

#### get\_publisher

```python
def get_publisher(name: str) -> M.Result[PublisherSpecification, Exception]
```

Get a publisher by name.

<a id="core.project.ContinuousDataFramework.get_script"></a>

#### get\_script

```python
def get_script(name: str) -> M.Result[ScriptSpecification, Exception]
```

Get a script by name.

<a id="core.project.ContinuousDataFramework.get_notebook"></a>

#### get\_notebook

```python
def get_notebook(name: str) -> M.Result[NotebookSpecification, Exception]
```

Get a notebook by name.

<a id="core.project.ContinuousDataFramework.get_gateways"></a>

#### get\_gateways

```python
def get_gateways() -> M.Result[t.Dict[str, GatewayConfig], Exception]
```

Convert the project's gateways to a dictionary.

<a id="core.project.ContinuousDataFramework.get_transform_context"></a>

#### get\_transform\_context

```python
def get_transform_context(sink: t.Optional[str] = None) -> sqlmesh.Context
```

Get a transform context for a sink.

<a id="core.project.Project"></a>

## Project Objects

```python
class Project(ContinuousDataFramework)
```

A CDF project.

<a id="core.project.Project.__init__"></a>

#### \_\_init\_\_

```python
def __init__(configuration: "dynaconf.Dynaconf",
             workspaces: t.Dict[str, "dynaconf.Dynaconf"]) -> None
```

Initialize a project.

<a id="core.project.Project.get_workspace"></a>

#### get\_workspace

```python
def get_workspace(name: str) -> M.Result["Workspace", Exception]
```

Get a workspace by name.

<a id="core.project.Project.get_workspace_from_path"></a>

#### get\_workspace\_from\_path

```python
def get_workspace_from_path(
        path: PathLike) -> M.Result["Workspace", Exception]
```

Get a workspace by path.

<a id="core.project.Project.load"></a>

#### load

```python
@classmethod
def load(cls, root: PathLike) -> "Project"
```

Create a project from a root path.

<a id="core.project.Workspace"></a>

## Workspace Objects

```python
class Workspace(ContinuousDataFramework)
```

A CDF workspace.

<a id="core.project.Workspace.__init__"></a>

#### \_\_init\_\_

```python
def __init__(name: str, *, project: Project) -> None
```

Initialize a workspace.

<a id="core.project.Workspace.parent"></a>

#### parent

```python
@property
def parent() -> Project
```

The parent project.

<a id="core.project.load_project"></a>

#### load\_project

Create a project from a root path.

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

<a id="core.feature_flag.launchdarkly.LaunchDarklyFlagProvider"></a>

## LaunchDarklyFlagProvider Objects

```python
class LaunchDarklyFlagProvider(BaseFlagProvider)
```

LaunchDarkly feature flag provider.

<a id="core.feature_flag.harness"></a>

# core.feature\_flag.harness

Harness feature flag provider.

<a id="core.feature_flag.harness.HarnessFlagProvider"></a>

## HarnessFlagProvider Objects

```python
class HarnessFlagProvider(BaseFlagProvider)
```

Harness feature flag provider.

<a id="core.feature_flag.harness.HarnessFlagProvider.drop"></a>

#### drop

```python
def drop(ident: str) -> str
```

Drop a feature flag.

<a id="core.feature_flag.harness.HarnessFlagProvider.create"></a>

#### create

```python
def create(ident: str, name: str) -> str
```

Create a feature flag.

<a id="core.feature_flag.harness.HarnessFlagProvider.apply_source"></a>

#### apply\_source

```python
def apply_source(source: DltSource) -> DltSource
```

Apply the feature flags to a dlt source.

<a id="core.feature_flag"></a>

# core.feature\_flag

Feature flag providers.

<a id="core.feature_flag.file"></a>

# core.feature\_flag.file

File-based feature flag provider.

<a id="core.feature_flag.file.FileFlagProvider"></a>

## FileFlagProvider Objects

```python
class FileFlagProvider(BaseFlagProvider)
```

<a id="core.feature_flag.file.FileFlagProvider.apply_source"></a>

#### apply\_source

```python
def apply_source(source: "DltSource") -> "DltSource"
```

Apply the feature flags to a dlt source.

<a id="core.feature_flag.noop"></a>

# core.feature\_flag.noop

No-op feature flag provider.

<a id="core.feature_flag.noop.NoopFlagProvider"></a>

## NoopFlagProvider Objects

```python
class NoopFlagProvider(BaseFlagProvider)
```

LaunchDarkly feature flag provider.

<a id="core.feature_flag.base"></a>

# core.feature\_flag.base

<a id="core.feature_flag.base.BaseFlagProvider"></a>

## BaseFlagProvider Objects

```python
class BaseFlagProvider(pydantic.BaseModel, abc.ABC)
```

<a id="core.feature_flag.base.BaseFlagProvider.apply_source"></a>

#### apply\_source

```python
@abc.abstractmethod
def apply_source(source: "DltSource") -> "DltSource"
```

Apply the feature flags to a dlt source.

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

