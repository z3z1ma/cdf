# cdf

cdf (Continuous Data Flow) is a simple data framework built in Python. Internally we leverage dlt and sqlmesh in order
to provide a simple interface for data scientists and engineers to build data pipelines. The target audience of cdf
are data platform engineers who are looking to put together a unified data platform for their organization utilizing
best-in-class open source tools. We take the best in both data ingestion and data transformation and marry them together
in conjunction with a simple interface and opinionated design.

## Design


⚠️ the following is a work in progress and is subject to change ⚠️

Given the following configuration file:

`cdf_config.toml`
```toml
[sources.hackernews]
keywords = ["openai", "altman", "microsoft"]
start_date = "{{ yesterday() }}"
end_date = "{{ today() }}"
daily_load = true
```

It maps directly to the following code:

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

This demonstrates managing pipeline configuration.

---

The following code demonstrates running a pipeline:
```python
import dlt

from cdf.core.workspace import Project

project = Project.find_nearest(path="examples/advanced")
with project.datateam.get_runtime_source("hackernews") as source:
    p = dlt.pipeline("hackernews", destination="duckdb")
    info = p.run(source)

print(info)

```
---

Continuing to look at cdf configuration, we can see it centralizes config for both SQLMesh and dlt into a single file with some opinionated handling. We enable jinja templating and advise users to declare ALL configuration in this file. Secrets included, but deferred via `{{ env_var("SOME_VAR") }}` vs relying on dlt's Environ provider. This makes behvior more immediately obvious and flexible. You can also use the multitude of interesting jinja filters to DRY up your config.

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

### Sources

Sources are the entrypoint to the data pipeline. They are responsible for pulling data from external systems and
loading it into a staging area. Sources are defined in the `sources` directory of a `cdf` project. Each source
must export a `__CDF_SOURCE__` variable which contains a dictionary of source names to `CDFSourceWrapper` objects.

**CDFSourceWrapper**

The `CDFSourceWrapper` object contains the following fields:

- `factory`: a function which returns a `CDFSource` object. A `CDFSource` object is a subclass of `dlt.sources.DltSource`. We export a `partial` of the `dlt.source` decorator with the `_impl_cls` argument preset to the `CDFSource` class so you can use it directly in your source file. In that sense, using `cdf` sources is identical to `dlt` except you can leverage our `@cdf_source` decorator to make things a bit more concise. The `factory` is used to allow for lazy loading of the source. This is important because we don't want to import the source unless it's actually used. This speeds up the CLI and allows sources to perform heavier discovery operations at runtime if they are dynamic.

- `version`: an integer which represents the version of the source. This is appended to the target dataset name in order to allow for multiple versions of the same source to coexist in the same database. It also allows us to update a source and make significant changes without breaking downstream models or adopting a non standard versioning scheme.

- `owners`: a list of strings which represent the owners of the source. This is used in order to simplify managing a large number of sources and models across a large number of teams. It is also used to generate a `README.md` file for each source which contains the owners and a description of the source. It is also displayed in the CLI when there are issues.

- `description`: a string which describes the source. This is used in order to generate a `README.md` file for each source which contains the owners and a description of the source.

- `tags`: a list of strings which represent tags for the source. This is used in order to simplify managing a large number of sources and models across a large number of teams. It also permits us to query sources by tag in the CLI.

### Models

We leverage SQLMesh to define our models. This allows us to leverage the full power of SQL in order to transform data. We apply a set of opinionated behaviors to SQLMesh in order to simplify the process of building data pipelines. We also provide a set of macros which make it easy to build pipelines which are consistent and easy to maintain.

### Publishers

We expose a `@cdf_publisher` decorator which allows you to define a publisher which can be used to publish data from a model to an external system. This is useful for publishing data to external systems such as Salesforce or Asana. Publishers are defined in the `publishers` directory of a `cdf` project. The primary input to a publisher is a class or function which writes rows to a target system as well as a mapping of model fields to target system fields.
