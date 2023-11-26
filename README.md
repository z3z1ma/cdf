# cdf

cdf (Continuous Data Flow) is a simple data framework built in Python. Internally we leverage dlt and sqlmesh in order
to provide a simple interface for data scientists and engineers to build data pipelines. The target audience of cdf are
data platform engineers who are looking to put together a unified data platform for their organization utilizing
best-in-class open source tools. We take the best in both data ingestion and data transformation and marry them together
in conjunction with a simple interface and opinionated design.


## Features

- [x] Central CLI for managing one or more `Workspace`s consistently across teams
- [x] Centralized configuration for all components (dlt and sqlmesh included)
- [x] Templating for configuration
- [x] Strongly opinionated directory structure for organizing sources, models, and publishers
- [x] Automated venv creation and dependency management for workspaces
- [x] Feature flag support for sources allowing external management of extraction logic similar to Fivetran / Airbyte
- [x] Discovery command for dlt sources
- [x] Head command for dlt resources to preview data in your terminal while developing
- [x] Support for multiple versions of the same source
- [x] Ownership tracking for all components
- [x] Tagging for all components
- [ ] Automated README generation for all components (in progress)
- [x] A `run` command to quickly run any binary within any workspace with dependencies
- [ ] Stdin ingestion to duckdb for quickly loading data into a local database and inspecting inferred schema
- [ ] Support for data contracts (upstream in dlt)
- [ ] Support for end to end lineage
- [ ] Automated data platform / framework metadata tracking

## Design


⚠️ the following is a work in progress and is subject to change ⚠️



### Integrating a source

In order to integrate a source into cdf, you must export a `__CDF_SOURCE__` variable which contains a dictionary of
component names to metadata. A component name is a string by which the component can be referenced in the CLI. There are
two ways to do this outlined below.
```python
# This is the only addition required to an existing dlt source file to get the benefits of cdf
__CDF_SOURCE__ = {
    "hackernews": {
        # factory must be resolvable via cdf config, kwargs should be set to dlt.config.value or populated with defaults / via closure
        "factory": hn_search,
        "version": 1,
        "owners": ("qa-team"),
        "description": "Extracts hackernews data from an API.",
        "tags": ("live", "simple", "test"),
        "metrics": {
            "keyword_hits": {
                "count": lambda _, metric=0: metric + 1,
            }
        },
    }
}

# Also worth metioning, above is exactly the same as:
from cdf import export_sources, source_spec

export_sources(
    hacker_news=source_spec(
        factory=hn_search,
        version=1,
        owners=("qa-team"),
        description="Extracts hackernews data from an API.",
        tags=("live", "simple", "test"),
        metrics={
            "keyword_hits": {
                "count": lambda _, metric=0: metric + 1,
            }
        },
    )
)

# the difference being that the latter gives type hints and is more readable
# while the former requires no imports of cdf and is thus valid independent of the cdf package
```



### Configuring a Source

Given the following configuration file:

`cdf_config.toml`
```toml
[sources.hackernews]
keywords = ["openai", "altman", "microsoft"]
start_date = "{{ yesterday() }}"
end_date = "{{ today() }}"
daily_load = true
```

It maps directly to the following code where the above config is injected into the source function as keyword arguments:

`sources/hackernews.py`
```python
@dlt.source(name="hackernews")
def hn_search(
    keywords=dlt.config.value,    # this MUST exist in the config
    start_date=dlt.config.value,  # this MUST exist in the config
    end_date=datetime.today(),    # this can be overridden by the config
    text="any",                   # this can be overridden by the config
    daily_load=False,             # this can be overridden by the config
):
    ...
```

This demonstrates managing pipeline configuration. Users familiar with dlt will recognize the semantic, however cdf
provides some opinionated design and features on top of this. Namely all config is in a top level `cdf_config.toml` file
relative to the workspace. Furthermore the config is jinja templated and can be overridden by environment variables
explicitly. This deviates from dlt's default behavior of expecting env vars to follow a specific naming convention. By
centralizing config, its easier to reason about. We can see all the kwargs and their exact destinations in one place.
And we can use more intuitive variable naming. Furthermore, you can use loops and conditionals in your config similar to
how kubernetes uses go templates. This is useful for managing multiple sources with similar config.


Continuing to look at cdf configuration, we can see it centralizes config for both SQLMesh and dlt into a single file.
This is another key difference vs using these two packages independently.

```toml
[sources.hackernews]
keywords = ["openai", "altman", "microsoft"]
start_date = "{{ yesterday() }}"
end_date = "{{ today() }}"
daily_load = true

[transforms]
default_gateway = "local"
model_defaults.dialect = "duckdb"

[transforms.gateways.local.connection]
type = "duckdb"
database = "cdf.duckdb"

```




### Running a pipeline via programmatic API

We provide 2 core classes that give us most of our functionality. `Project` and `Workspace`. Using cdf is as simple as
instantiating a `Project` object and then using it to access one or more `Workspace` objects. A `Workspace` object
represents a collection of sources, models, and publishers which is an amalgamation of dlt, sqlmesh, and cdf specific
componentry. A workspace is an opinionated layout with specific naming conventions which gives end users less to think
about and makes any project leveraging cdf more consistent and easier to reason about.


The following code demonstrates running a pipeline:
```python
from cdf import Project, pipeline

project = Project.find_nearest(path="examples/advanced")
source = project.datateam.sources["hackernews"]
with project.datateam.overlay():
    load_info = pipeline("test").run(source(), destination="duckdb")
print(load_info)
print(source.runtime_metrics)
```

---

### Sources

Sources are the entrypoint to the data pipeline. They are responsible for pulling data from external systems and loading
it into a staging area. Sources are defined in the `sources` directory of a `cdf` project. Each source must export a
`__CDF_SOURCE__` variable which contains a dictionary of source names to `CDFSourceWrapper` objects.

**CDFSourceWrapper**

The `CDFSourceWrapper` object contains the following fields:

- `factory`: a function which returns a `CDFSource` object. A `CDFSource` object is a subclass of
`dlt.sources.DltSource`. We export a `partial` of the `dlt.source` decorator with the `_impl_cls` argument preset to the
`CDFSource` class so you can use it directly in your source file. In that sense, using `cdf` sources is identical to
`dlt` except you can leverage our `@cdf_source` decorator to make things a bit more concise. The `factory` is used to
allow for lazy loading of the source. This is important because we don't want to import the source unless it's actually
used. This speeds up the CLI and allows sources to perform heavier discovery operations at runtime if they are dynamic.

- `version`: an integer which represents the version of the source. This is appended to the target dataset name in order
to allow for multiple versions of the same source to coexist in the same database. It also allows us to update a source
and make significant changes without breaking downstream models or adopting a non standard versioning scheme.

- `owners`: a list of strings which represent the owners of the source. This is used in order to simplify managing a
large number of sources and models across a large number of teams. It is also used to generate a `README.md` file for
each source which contains the owners and a description of the source. It is also displayed in the CLI when there are
issues.

- `description`: a string which describes the source. This is used in order to generate a `README.md` file for each
source which contains the owners and a description of the source.

- `tags`: a list of strings which represent tags for the source. This is used in order to simplify managing a large
number of sources and models across a large number of teams. It also permits us to query sources by tag in the CLI.

### Models

We leverage SQLMesh to define our models. This allows us to leverage the full power of SQL in order to transform data.
We apply a set of opinionated behaviors to SQLMesh in order to simplify the process of building data pipelines. We also
provide a set of macros which make it easy to build pipelines which are consistent and easy to maintain.

### Publishers

We expose a `@cdf_publisher` decorator which allows you to define a publisher which can be used to publish data from a
model to an external system. This is useful for publishing data to external systems such as Salesforce or Asana.
Publishers are defined in the `publishers` directory of a `cdf` project. The primary input to a publisher is a class or
function which writes rows to a target system as well as a mapping of model fields to target system fields.
