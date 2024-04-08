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
* `--help`: Show this message and exit.

Made with [red]♥[/red] by [bold]z3z1ma[/bold].

**Commands**:

* `discover`: :mag: Evaluates a :zzz: Lazy [b...
* `execute-notebook`: :notebook: Execute a [b yellow]Notebook[/b...
* `execute-script`: :hammer: Execute a [b yellow]Script[/b...
* `head`: :wrench: Prints the first N rows of a [b...
* `index`: :page_with_curl: Print an index of...
* `init`: :art: Initialize a new project.
* `jupyter-lab`: :notebook: Start a Jupyter Lab server.
* `pipeline`: :inbox_tray: Ingest data from a [b...
* `publish`: :outbox_tray: [b yellow]Publish[/b yellow]...
* `spec`: :mag: Print the fields for a given spec type.

## `cdf discover`

:mag: Evaluates a :zzz: Lazy [b blue]pipeline[/b blue] and enumerates the discovered resources.


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

## `cdf execute-notebook`

:notebook: Execute a [b yellow]Notebook[/b yellow] within the context of the current workspace.


Args:
    ctx: The CLI context.
    notebook: The notebook to execute.
    params: The parameters to pass to the notebook as a json formatted string.

**Usage**:

```console
$ cdf execute-notebook [OPTIONS] NOTEBOOK
```

**Arguments**:

* `NOTEBOOK`: The notebook to execute.  [required]

**Options**:

* `--params TEXT`: The parameters to pass to the notebook as a json formatted string.  [default: {}]
* `--help`: Show this message and exit.

## `cdf execute-script`

:hammer: Execute a [b yellow]Script[/b yellow] within the context of the current workspace.


Args:
    ctx: The CLI context.
    script: The script to execute.
    quiet: Whether to suppress the script stdout.

**Usage**:

```console
$ cdf execute-script [OPTIONS] SCRIPT
```

**Arguments**:

* `SCRIPT`: The script to execute.  [required]

**Options**:

* `--quiet / --no-quiet`: Suppress the script stdout.  [default: no-quiet]
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

:notebook: Start a Jupyter Lab server.

**Usage**:

```console
$ cdf jupyter-lab [OPTIONS]
```

**Options**:

* `--help`: Show this message and exit.

## `cdf pipeline`

:inbox_tray: Ingest data from a [b blue]pipeline[/b blue] into a data store where it can be [b red]Transformed[/b red].


Args:
    ctx: The CLI context.
    pipeline_to_sink: The pipeline and sink separated by a colon.
    select: The resources to ingest as a sequence of glob patterns.
    exclude: The resources to exclude as a sequence of glob patterns.
    force_replace: Whether to force replace the write disposition.
    no_stage: Whether to disable staging the data in the sink.

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

## `cdf spec`

:mag: Print the fields for a given spec type.


Args:
    name: The name of the spec to print.
    json_schema: Whether to print the JSON schema for the spec.

**Usage**:

```console
$ cdf spec [OPTIONS] NAME:{pipeline|publisher|script|notebook|sink|feature_flags}
```

**Arguments**:

* `NAME:{pipeline|publisher|script|notebook|sink|feature_flags}`: [required]

**Options**:

* `--json-schema / --no-json-schema`: [default: no-json-schema]
* `--help`: Show this message and exit.

