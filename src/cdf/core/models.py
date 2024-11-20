"""The models module defines pydantic models which concretely define the configuration schema for the CDF project."""

from __future__ import annotations

import typing as t
from pathlib import Path

import pydantic

import cdf.core.constants as c


class _CDFConfigModel(pydantic.BaseModel, arbitrary_types_allowed=True, from_attributes=True):
    pass


# https://dlthub.com/docs/intro
# https://github.com/dlt-hub/dlt
class DltAdapterConfig(_CDFConfigModel):
    """Configuration for the DLT adapter."""

    adapter: t.Literal["dlt"] = "dlt"

    params: dict[str, t.Any] = {}
    """Parameters to merge into dlt config, ie extract.worker, normalize.pool_type"""


# https://github.com/singer-io/getting-started
class SingerAdapterConfig(_CDFConfigModel):
    """Configuration for the Singer adapter."""

    adapter: t.Literal["singer"] = "singer"

    tap: str
    """Singer tap to use for extracting data"""

    tap_config: dict[str, t.Any] = {}
    """Configuration for the tap which will be serialized as a JSON file on disk"""

    target: str
    """Singer target to use for loading data"""

    target_config: dict[str, t.Any] = {}
    """Configuration for the target which will be serialized as a JSON file on disk"""

    catalog: dict[str, t.Any] | None = None
    """Catalog to use for the tap, will be serialized as a JSON file on disk."""

    state: dict[str, t.Any] | None = None
    """State to use for the tap, will be serialized as a JSON file on disk."""

    properties: dict[str, t.Any] | None = None
    """Properties to select or deselect data from the tap."""

    env: dict[str, str] | None = None
    """Environment variables to set during the execution of the tap and target."""


class _SlingReplicationStreamConfig(_CDFConfigModel):
    mode: str | None = None
    """The target load mode to use: incremental, truncate, full-refresh, backfill or snapshot. Default is full-refresh."""

    object_: str | None = pydantic.Field(
        None, alias="object", description="The source object to replicate."
    )
    """The target table (schema.table) or local/cloud file path. Use file:// for local paths."""

    select: list[str] | None = None
    """Select or exclude specific columns from the source stream. Use - prefix to exclude."""

    primary_key: list[str] | None = None
    """The column(s) to use as primary key. If composite key, use array."""

    update_key: str | None = None
    """The column to use as update key (for incremental mode)."""

    single: bool | None = None
    """When using a wildcard (*) in the stream name, consider as a single stream (don't expand into many streams)."""

    sql: str | None = None
    """The custom SQL query to use. Accepts file://path/to.query.sql as well."""

    source_options: dict[str, t.Any] | None = None
    """Options to further configure source."""

    target_options: dict[str, t.Any] | None = None
    """Options to further configure target."""

    disabled: bool = False
    """Flag to disable this replication stream."""


# https://slingdata.io/
# https://github.com/slingdata-io/sling-cli
class SlingAdapterConfig(_CDFConfigModel):
    """Configuration for the Sling adapter."""

    adapter: t.Literal["sling"] = "sling"

    source: str
    """The source database connection (name, conn string or URL)."""

    target: str
    """The target database connection (name, conn string or URL)."""

    defaults: _SlingReplicationStreamConfig
    """Default configuration for replication streams."""

    streams: dict[str, _SlingReplicationStreamConfig]
    """Mapping of stream keys to their replication stream configurations."""

    env: dict[str, t.Any] | None = None
    """Environment variables to use for replication."""


ExtractLoadConfig = DltAdapterConfig | SingerAdapterConfig | SlingAdapterConfig


class PytestAdapterConfig(_CDFConfigModel):
    adapter: t.Literal["pytest"] = "pytest"

    test_files: list[str] = []
    """List of test files or directories to run."""

    pytest_options: list[str] = []
    """Additional command-line options to pass to pytest."""


class UnittestAdapterConfig(_CDFConfigModel):
    adapter: t.Literal["unittest"] = "unittest"

    test_files: list[str] = []
    """List of test files or directories to run."""

    unittest_options: list[str] = []
    """Additional command-line options to pass to unittest."""


class DbtTestAdapterConfig(_CDFConfigModel):
    adapter: t.Literal["dbt"] = "dbt"

    project_dir: str | None = None
    """Path to the dbt project directory."""

    profiles_dir: str | None = None
    """Path to the dbt profiles directory."""

    target: str | None = None
    """Target profile to use for dbt."""

    vars: dict[str, t.Any] = {}
    """Variables to pass to dbt."""

    models: list[str] = []
    """List of models to include in the test."""

    exclude: list[str] = []
    """List of models to exclude from the test."""

    threads: int | None = None
    """Number of threads to use during dbt execution."""


TestConfig = PytestAdapterConfig | UnittestAdapterConfig | DbtTestAdapterConfig


class DbtTransformAdapterConfig(_CDFConfigModel):
    adapter: t.Literal["dbt"] = "dbt"

    project_dir: str | None = None
    """Path to the dbt project directory."""

    profiles_dir: str | None = None
    """Path to the dbt profiles directory."""

    target: str | None = None
    """Target profile to use for dbt."""

    vars: dict[str, t.Any] = {}
    """Variables to pass to dbt."""

    models: list[str] = []
    """List of models to include in the transform."""

    exclude: list[str] = []
    """List of models to exclude from the transform."""

    full_refresh: bool = False
    """Whether to perform a full refresh of incremental models."""

    threads: int | None = None
    """Number of threads to use during dbt execution."""


class SqlMeshAdapterConfig(_CDFConfigModel):
    adapter: t.Literal["sqlmesh"] = "sqlmesh"

    config_path: str | None = None
    """Path to the SqlMesh configuration file."""

    environment: str | None = None
    """Environment name to use for SqlMesh."""

    execution_time: str | None = None
    """Execution time for the SqlMesh run."""

    variables: dict[str, t.Any] = {}
    """Variables to pass to SqlMesh."""


class JinjaSqlAdapterConfig(_CDFConfigModel):
    adapter: t.Literal["jinja_sql"] = "jinja_sql"

    template_path: str
    """Path to the Jinja SQL templates."""

    variables: dict[str, t.Any] = {}
    """Variables to pass to the templates."""


TransformConfig = DbtTransformAdapterConfig | SqlMeshAdapterConfig | JinjaSqlAdapterConfig


class PackageManifest(_CDFConfigModel):
    """Describes the outputs of a data package and provides metadata for interaction."""

    pipeline_name: str
    """Name of the data pipeline"""

    dataset_names: list[str]
    """List of raw dataset names generated in the package"""

    destination_name: str
    """Name of the project destination where data will be loaded"""

    output_dataset_names: list[str] | None = None
    """Optional list of output dataset names, otherwise assumed to be the same as dataset_names"""

    public_tables: list[str] = []
    """List of explicit public tables in the package output"""

    pii_column_patterns: list[
        str
    ] = []  # Given a .schema method in the adapter, we can auto-tag tables
    """List of patterns for columns containing PII (Personally Identifiable Information)"""

    owner: str
    """Data owner or maintainer"""

    description: str | None = None
    """Package description"""

    tags: list[str] = []
    """List of tags for the package"""

    version: str = "0.1.0"
    """Semantic versioning"""

    external_references: list[str] = []
    """Links to related resources, docs, or tickets"""

    dependencies: list[str] = []  # Can be used to auto-detect breaking changes
    """Other datasets/packages this depends on"""

    dependents: list[str] = []  # Can be used to auto-detect breaking changes
    """Datasets/packages depending on this one"""

    runtime_parameters: dict[str, t.Any] = {}
    """Key-value pairs for runtime parameters used by cdf internally"""

    compliance_labels: list[str] = []
    """e.g., GDPR, HIPAA, SOC2"""

    security_requirements: list[str] = []
    """e.g., encryption, access controls"""

    validation_rules: dict[
        str, str
    ] = {}  # We can use duckdb to check these during EL? Otherwise defer to transform
    """e.g., { "rule_name": "SQL or DSL query for validation" }"""


class DataPackageConfig(_CDFConfigModel):
    """A package comprising a single data source with adapters to extract, load, transform, and test data."""

    schedules: list[str] = []
    """List of schedules for the data package, for use with Scheduler adapter"""

    manifest: PackageManifest
    """Manifest describing the data package outputs and metadata"""

    extract_load: ExtractLoadConfig | None = None
    """Configuration for the extract and load adapter"""

    transform: TransformConfig | None = None
    """Configuration for the transform adapter"""

    test: TestConfig | None = None
    """Configuration for the test adapter"""


class ProjectConfig(_CDFConfigModel):
    """A project contains data packages"""

    dependencies_dir: Path = pydantic.Field(default=c.DEFAULT_DEPENDENCIES_DIR)
    """Directory where dependencies are stored"""

    data_packages_dir: Path = pydantic.Field(default=c.DEFAULT_DATA_PACKAGES_DIR)
    """Directory where data packages are stored"""

    transform: TransformConfig | None = None
    """Configuration for the global transform adapter which should be able to model and combine data across packages"""

    test: TestConfig | None = None
    """Configuration for the project test adapter for more holistic tests"""

    global_tags: list[str] = []
    """Tags applicable to all packages"""

    global_runtime_parameters: dict[str, t.Any] = {}
    """Runtime defaults for all packages used by cdf internally"""

    # TODO: add state backends
    ...
