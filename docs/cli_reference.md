# `cdf`

CDF (continuous data framework) is a framework for end to end data processing.

**Usage**:

```console
$ cdf [OPTIONS] WORKSPACE COMMAND [ARGS]...
```

**Arguments**:

* `WORKSPACE`: [required]

**Options**:

* `-p, --path PATH`: Path to the project.  [env var: CDF_ROOT; default: .]
* `-d, --debug`: Enable debug mode.
* `-e, --env TEXT`: Environment to use.
* `-l, --log-level TEXT`: The log level to use.  [env var: LOG_LEVEL]
* `--help`: Show this message and exit.

Made with [red]♥[/red] by [bold]z3z1ma[/bold].

**Commands**:

* `discover`: :mag: Dry run a [b blue]Pipeline[/b blue]...
* `head`: :wrench: Prints the first N rows of a [b...
* `index`: :page_with_curl: Print an index of...
* `init`: :art: Initialize a new project.
* `jupyter-lab`: :star2: Start a Jupyter Lab server in the...
* `model`: :construction: Model management commands.
* `notebook`: :notebook: Execute a [b yellow]Notebook[/b...
* `pipeline`: :inbox_tray: Ingest data from a [b...
* `publish`: :outbox_tray: [b yellow]Publish[/b yellow]...
* `schema`: :construction: Schema management commands.
* `script`: :hammer: Execute a [b yellow]Script[/b...
* `spec`: :blue_book: Print the fields for a given...
* `state`: :construction: State management commands.

## `cdf discover`

:mag: Dry run a [b blue]Pipeline[/b blue] and enumerates the discovered resources.


Args:
    ctx: The CLI context.
    pipeline: The pipeline in which to discover resources.
    no_quiet: Whether to suppress the pipeline stdout.

**Usage**:

```console
$ cdf discover [OPTIONS] PIPELINE
```

**Arguments**:

* `PIPELINE`: The pipeline in which to discover resources.  [required]

**Options**:

* `--no-quiet / --no-no-quiet`: Pipeline stdout is suppressed by default, this disables that.  [default: no-no-quiet]
* `--help`: Show this message and exit.

## `cdf head`

:wrench: Prints the first N rows of a [b green]Resource[/b green] within a [b blue]pipeline[/b blue]. Defaults to [cyan]5[/cyan].

This is useful for quickly inspecting data :detective: and verifying that it is coming over the wire correctly.


Args:
    ctx: The CLI context.
    pipeline: The pipeline to inspect.
    resource: The resource to inspect.
    n: The number of rows to print.

Raises:
    typer.BadParameter: If the resource is not found in the pipeline.

**Usage**:

```console
$ cdf head [OPTIONS] PIPELINE RESOURCE
```

**Arguments**:

* `PIPELINE`: The pipeline to inspect.  [required]
* `RESOURCE`: The resource to inspect.  [required]

**Options**:

* `-n, --rows INTEGER`: [default: 5]
* `--help`: Show this message and exit.

## `cdf index`

:page_with_curl: Print an index of [b][blue]Pipelines[/blue], [red]Models[/red], [yellow]Publishers[/yellow][/b], and other components.

**Usage**:

```console
$ cdf index [OPTIONS]
```

**Options**:

* `--hydrate / --no-hydrate`: [default: no-hydrate]
* `--help`: Show this message and exit.

## `cdf init`

:art: Initialize a new project.

**Usage**:

```console
$ cdf init [OPTIONS]
```

**Options**:

* `--help`: Show this message and exit.

## `cdf jupyter-lab`

:star2: Start a Jupyter Lab server in the context of a workspace.

**Usage**:

```console
$ cdf jupyter-lab [OPTIONS]
```

**Options**:

* `--help`: Show this message and exit.

## `cdf model`

:construction: Model management commands.

**Usage**:

```console
$ cdf model [OPTIONS] COMMAND [ARGS]...
```

**Options**:

* `--help`: Show this message and exit.

Made with [red]♥[/red] by [bold]z3z1ma[/bold].

**Commands**:

* `diff`: :bar_chart: Compute the diff of a [b...
* `evaluate`: :bar_chart: Evaluate a [b red]Model[/b...
* `name`: :bar_chart: Get a [b red]Model[/b red]'s...
* `prototype`: :bar_chart: Prototype a model and save the...
* `render`: :bar_chart: Render a [b red]Model[/b red]...

### `cdf model diff`

:bar_chart: Compute the diff of a [b red]Model[/b red] across 2 environments. A thin wrapper around `sqlmesh table_diff`


Args:
    ctx: The CLI context.
    model: The model to evaluate. Can be prefixed with the gateway.
    source_target: The source and target environments separated by a colon.

**Usage**:

```console
$ cdf model diff [OPTIONS] MODEL SOURCE_TARGET
```

**Arguments**:

* `MODEL`: The model to evaluate. Can be prefixed with the gateway.  [required]
* `SOURCE_TARGET`: The source and target environments separated by a colon.  [required]

**Options**:

* `--show-sample / --no-show-sample`: Whether to show a sample of the diff.  [default: no-show-sample]
* `--help`: Show this message and exit.

### `cdf model evaluate`

:bar_chart: Evaluate a [b red]Model[/b red] and print the results. A thin wrapper around `sqlmesh evaluate`


Args:
    ctx: The CLI context.
    model: The model to evaluate. Can be prefixed with the gateway.
    limit: The number of rows to limit the evaluation to.

**Usage**:

```console
$ cdf model evaluate [OPTIONS] MODEL
```

**Arguments**:

* `MODEL`: The model to evaluate. Can be prefixed with the gateway.  [required]

**Options**:

* `--start TEXT`: The start time to evaluate the model from. Defaults to 1 month ago.  [default: 1 month ago]
* `--end TEXT`: The end time to evaluate the model to. Defaults to now.  [default: now]
* `--limit INTEGER`: The number of rows to limit the evaluation to.
* `--help`: Show this message and exit.

### `cdf model name`

:bar_chart: Get a [b red]Model[/b red]'s physical table name. A thin wrapper around `sqlmesh table_name`


Args:
    ctx: The CLI context.
    model: The model to evaluate. Can be prefixed with the gateway.

**Usage**:

```console
$ cdf model name [OPTIONS] MODEL
```

**Arguments**:

* `MODEL`: The model to convert the physical name. Can be prefixed with the gateway.  [required]

**Options**:

* `--help`: Show this message and exit.

### `cdf model prototype`

:bar_chart: Prototype a model and save the results to disk.


Args:
    ctx: The CLI context.
    dependencies: The dependencies to include in the prototype.
    start: The start time to evaluate the model from. Defaults to 1 month ago.
    end: The end time to evaluate the model to. Defaults to now.
    limit: The number of rows to limit the evaluation to.

**Usage**:

```console
$ cdf model prototype [OPTIONS]
```

**Options**:

* `-d, --dependencies TEXT`: The dependencies to include in the prototype.
* `--start TEXT`: The start time to evaluate the model from. Defaults to 1 month ago.  [default: 1 month ago]
* `--end TEXT`: The end time to evaluate the model to. Defaults to now.  [default: now]
* `--limit INTEGER`: The number of rows to limit the evaluation to.  [default: 5000000]
* `--help`: Show this message and exit.

### `cdf model render`

:bar_chart: Render a [b red]Model[/b red] and print the query. A thin wrapper around `sqlmesh render`


Args:
    ctx: The CLI context.
    model: The model to evaluate. Can be prefixed with the gateway.
    start: The start time to evaluate the model from. Defaults to 1 month ago.
    end: The end time to evaluate the model to. Defaults to now.
    expand: The referenced models to expand.
    dialect: The SQL dialect to use for rendering.

**Usage**:

```console
$ cdf model render [OPTIONS] MODEL
```

**Arguments**:

* `MODEL`: The model to evaluate. Can be prefixed with the gateway.  [required]

**Options**:

* `--start TEXT`: The start time to evaluate the model from. Defaults to 1 month ago.  [default: 1 month ago]
* `--end TEXT`: The end time to evaluate the model to. Defaults to now.  [default: now]
* `--expand TEXT`: The referenced models to expand.
* `--dialect TEXT`: The SQL dialect to use for rendering.
* `--help`: Show this message and exit.

## `cdf notebook`

:notebook: Execute a [b yellow]Notebook[/b yellow] within the context of the current workspace.


Args:
    ctx: The CLI context.
    notebook: The notebook to execute.
    params: The parameters to pass to the notebook as a json formatted string.

**Usage**:

```console
$ cdf notebook [OPTIONS] NOTEBOOK
```

**Arguments**:

* `NOTEBOOK`: The notebook to execute.  [required]

**Options**:

* `--params TEXT`: The parameters to pass to the notebook as a json formatted string.  [default: {}]
* `--help`: Show this message and exit.

## `cdf pipeline`

:inbox_tray: Ingest data from a [b blue]Pipeline[/b blue] into a data store where it can be [b red]Transformed[/b red].


Args:
    ctx: The CLI context.
    pipeline_to_sink: The pipeline and sink separated by a colon.
    select: The resources to ingest as a sequence of glob patterns.
    exclude: The resources to exclude as a sequence of glob patterns.
    force_replace: Whether to force replace the write disposition.
    no_stage: Allows selective disabling of intermediate staging even if configured in sink.

**Usage**:

```console
$ cdf pipeline [OPTIONS] PIPELINE_TO_SINK
```

**Arguments**:

* `PIPELINE_TO_SINK`: The pipeline and sink separated by a colon.  [required]

**Options**:

* `-s, --select TEXT`: Glob pattern for resources to run. Can be specified multiple times.  [default: (dynamic)]
* `-x, --exclude TEXT`: Glob pattern for resources to exclude. Can be specified multiple times.  [default: (dynamic)]
* `-F, --force-replace`: Force the write disposition to replace ignoring state. Useful to force a reload of incremental resources.
* `--no-stage`: Do not stage the data in the staging destination of the sink even if defined.
* `--help`: Show this message and exit.

## `cdf publish`

:outbox_tray: [b yellow]Publish[/b yellow] data from a data store to an [violet]External[/violet] system.


Args:
    ctx: The CLI context.
    sink_to_publisher: The sink and publisher separated by a colon.
    skip_verification: Whether to skip the verification of the publisher dependencies.

**Usage**:

```console
$ cdf publish [OPTIONS] SINK_TO_PUBLISHER
```

**Arguments**:

* `SINK_TO_PUBLISHER`: The sink and publisher separated by a colon.  [required]

**Options**:

* `--skip-verification / --no-skip-verification`: Skip the verification of the publisher dependencies.  [default: no-skip-verification]
* `--help`: Show this message and exit.

## `cdf schema`

:construction: Schema management commands.

**Usage**:

```console
$ cdf schema [OPTIONS] COMMAND [ARGS]...
```

**Options**:

* `--help`: Show this message and exit.

Made with [red]♥[/red] by [bold]z3z1ma[/bold].

**Commands**:

* `dump`: :computer: Dump the schema of a [b...
* `edit`: :pencil: Edit the schema of a [b...

### `cdf schema dump`

:computer: Dump the schema of a [b blue]pipeline[/b blue]:[violet]sink[/violet] combination.


Args:
    ctx: The CLI context.
    pipeline_to_sink: The pipeline:sink combination from which to fetch the schema.
    format: The format to dump the schema in.

Raises:
    typer.BadParameter: If the pipeline or sink are not found.

**Usage**:

```console
$ cdf schema dump [OPTIONS] PIPELINE_TO_SINK
```

**Arguments**:

* `PIPELINE_TO_SINK`: The pipeline:sink combination from which to fetch the schema.  [required]

**Options**:

* `--format [json|yaml|yml|py|python|dict]`: The format to dump the schema in.  [default: json]
* `--help`: Show this message and exit.

### `cdf schema edit`

:pencil: Edit the schema of a [b blue]pipeline[/b blue]:[violet]sink[/violet] combination using the system editor.


Args:
    ctx: The CLI context.
    pipeline_to_sink: The pipeline:sink combination from which to fetch the schema.

Raises:
    typer.BadParameter: If the pipeline or sink are not found.

**Usage**:

```console
$ cdf schema edit [OPTIONS] PIPELINE_TO_SINK
```

**Arguments**:

* `PIPELINE_TO_SINK`: The pipeline:sink combination from which to fetch the schema.  [required]

**Options**:

* `--help`: Show this message and exit.

## `cdf script`

:hammer: Execute a [b yellow]Script[/b yellow] within the context of the current workspace.


Args:
    ctx: The CLI context.
    script: The script to execute.
    quiet: Whether to suppress the script stdout.

**Usage**:

```console
$ cdf script [OPTIONS] SCRIPT
```

**Arguments**:

* `SCRIPT`: The script to execute.  [required]

**Options**:

* `--quiet / --no-quiet`: Suppress the script stdout.  [default: no-quiet]
* `--help`: Show this message and exit.

## `cdf spec`

:blue_book: Print the fields for a given spec type.


Args:
    name: The name of the spec to print.
    json_schema: Whether to print the JSON schema for the spec.

**Usage**:

```console
$ cdf spec [OPTIONS] NAME:{pipeline|publisher|script|notebook|sink|feature_flags|filesystem}
```

**Arguments**:

* `NAME:{pipeline|publisher|script|notebook|sink|feature_flags|filesystem}`: [required]

**Options**:

* `--json-schema / --no-json-schema`: [default: no-json-schema]
* `--help`: Show this message and exit.

## `cdf state`

:construction: State management commands.

**Usage**:

```console
$ cdf state [OPTIONS] COMMAND [ARGS]...
```

**Options**:

* `--help`: Show this message and exit.

Made with [red]♥[/red] by [bold]z3z1ma[/bold].

**Commands**:

* `dump`: :computer: Dump the state of a [b...
* `edit`: :pencil: Edit the state of a [b...

### `cdf state dump`

:computer: Dump the state of a [b blue]pipeline[/b blue]:[violet]sink[/violet] combination.


Args:
    ctx: The CLI context.
    pipeline_to_sink: The pipeline:sink combination from which to fetch the state.

Raises:
    typer.BadParameter: If the pipeline or sink are not found.

**Usage**:

```console
$ cdf state dump [OPTIONS] PIPELINE_TO_SINK
```

**Arguments**:

* `PIPELINE_TO_SINK`: The pipeline:sink combination from which to fetch the schema.  [required]

**Options**:

* `--help`: Show this message and exit.

### `cdf state edit`

:pencil: Edit the state of a [b blue]pipeline[/b blue]:[violet]sink[/violet] combination using the system editor.


Args:
    ctx: The CLI context.
    pipeline_to_sink: The pipeline:sink combination from which to fetch the state.

Raises:
    typer.BadParameter: If the pipeline or sink are not found.

**Usage**:

```console
$ cdf state edit [OPTIONS] PIPELINE_TO_SINK
```

**Arguments**:

* `PIPELINE_TO_SINK`: The pipeline:sink combination from which to fetch the state.  [required]

**Options**:

* `--help`: Show this message and exit.

