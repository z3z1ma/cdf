"""The models module defines pydantic models which concretely define the configuration schema for the CDF project."""

from __future__ import annotations

import typing as t
from collections.abc import Iterable
from pathlib import Path

import pydantic

import cdf.core.constants as c
from cdf.commons.pyutils import resolve_entry_point

__all__ = [
    "DltAdapterConfig",
    "SingerAdapterConfig",
    "SlingSourceOptions",
    "SlingTargetOptions",
    "SlingReplicationStreamConfig",
    "SlingAdapterConfig",
    "HamiltonAdapterConfig",
    "PytestAdapterConfig",
    "UnittestAdapterConfig",
    "DbtTestAdapterConfig",
    "DbtTransformAdapterConfig",
    "SqlMeshAdapterConfig",
    "JinjaSqlAdapterConfig",
    "PackageManifest",
    "DataPackageConfig",
    "ProjectConfig",
]


class _CDFConfigModel(pydantic.BaseModel, arbitrary_types_allowed=True, from_attributes=True):
    pass


# https://dlthub.com/docs/intro
# https://github.com/dlt-hub/dlt
class DltAdapterConfig(_CDFConfigModel):
    """Configuration for the DLT adapter."""

    adapter: t.Literal["dlt"] = "dlt"

    pipeline_name: str
    """Name of the data pipeline"""
    params: dict[str, t.Any] = {}
    """Parameters to merge into dlt config, ie extract.worker, normalize.pool_type"""


# https://github.com/singer-io/getting-started
class SingerAdapterConfig(_CDFConfigModel):
    """Configuration for the Singer adapter."""

    adapter: t.Literal["singer"] = "singer"

    tap_name: str
    """Singer tap to use for extracting data"""
    target_name: str
    """Singer target to use for loading data"""
    tap_requirements: str | list[str] | None = None
    """The pip compatible requirements for the tap, otherwise the tap_name is used"""
    target_requirements: str | list[str] | None = None
    """The pip compatible requirements for the target, otherwise the target_name is used"""
    tap_config: dict[str, t.Any] = {}
    """Configuration for the tap which will be serialized as a JSON file on disk"""
    target_config: dict[str, t.Any] = {}
    """Configuration for the target which will be serialized as a JSON file on disk"""
    tap_catalog: dict[str, t.Any] | None = None
    """Catalog to use for the tap, will be serialized as a JSON file on disk."""
    env: dict[str, str] | None = None
    """Environment variables to set during the execution of the tap and target."""


# https://docs.slingdata.io/sling-cli/run/configuration#source
class SlingSourceOptions(_CDFConfigModel):
    """Source options for a replication stream in Sling."""

    columns: dict[str, str]
    """Specify the type that a column should be cast as. The map key is the column name, and the value is the type."""
    compression: str | None = "auto"
    """The type of compression to use when reading files. Valid inputs are none, auto, gzip, zstd, snappy. Default is auto."""
    datetime_format: str = "auto"
    """The ISO 8601 date format to use when reading date values. Default is auto."""
    delimiter: str = "auto"
    """The delimiter to use when parsing tabular files. Default is auto."""
    escape: str = '"'
    """The escape character to use when parsing tabular files. Default is "."""
    empty_as_null: bool | None = True
    """Whether empty fields should be treated as NULL. Default is true."""
    flatten: bool | None = None
    """Whether to flatten a semi-structure file source format (JSON, XML)."""
    format: str | None = None
    """The format of the file(s). Options are: csv, parquet, xlsx, avro, json, jsonlines, sas7bdat, and xml."""
    header: bool | None = True
    """Whether to consider the first line as header. Default is true."""
    jmespath: str | None = None
    """Specify a JMESPath expression to use to filter/extract nested JSON data."""
    limit: int | None = None
    """The maximum number of rows to pull from the source."""
    max_decimals: int | None = None
    """The maximum number of decimal places to consider."""
    null_if: str | None = "NULL"
    """Whether this case-sensitive value should be treated as NULL when encountered. Default is NULL."""
    sheet: str | None = None
    """The name of the sheet to use as a data source, for example, Sheet1. Default is the first sheet."""
    range: str | None = None
    """The range to use for backfill mode, separated by a single comma. Example: 2021-01-01,2021-02-01 or 1,10000."""
    skip_blank_lines: bool | None = False
    """Whether blank lines should be skipped when encountered. Default is false."""
    transforms: list[str]
    """An object/map, or array/list of built-in transforms to apply to records."""
    trim_space: bool | None = False
    """Whether white spaces at the beginning or end of parsed values should be removed. Default is false."""


# https://docs.slingdata.io/sling-cli/run/configuration#target
class SlingTargetOptions(_CDFConfigModel):
    """Target options for a replication stream in Sling."""

    add_new_columns: bool | None = True
    """Whether to add new columns from stream not found in target table (when mode is not full-refresh). Default is true."""
    adjust_column_type: bool | None = False
    """Whether to adjust the column type when needed. Default is false. [BETA]"""
    batch_limit: int | None = None
    """The maximum number of records per transaction batch."""
    column_casing: str | None = "source"
    """Whether to convert the column name casing. Default is source."""
    compression: str | None = "auto"
    """The type of compression to use when writing files. Default is auto."""
    datetime_format: str = "auto"
    """The ISO 8601 date format to use when writing date values. Default is auto."""
    delimiter: str = ","
    """The delimiter to use when writing tabular files. Default is ,."""
    file_max_bytes: int = 16000000
    """The maximum number of bytes to write to a file. Default is 16000000."""
    file_max_rows: int = 500000
    """The maximum number of rows to write to a file. Default is 500000."""
    format: str | None = None
    """The format of the file(s). Options are: csv, parquet, xlsx, json and jsonlines."""
    header: bool | None = True
    """Whether to write the first line as header. Default is true."""
    post_sql: str | None = None
    """The SQL query to run after loading."""
    pre_sql: str | None = None
    """The SQL query to run before loading."""
    table_ddl: str | None = None
    """The table DDL to use when writing to a database. Default is auto-generated by Sling."""
    table_keys: dict[str, list[str]] | None = None
    """The table keys to define when creating a table."""
    table_tmp: str | None = None
    """The temporary table name that should be used when loading into a database. Default is auto-generated by Sling."""
    use_bulk: bool | None = True
    """Whether to use external bulk loading tools. Default is true."""


class SlingReplicationStreamConfig(_CDFConfigModel):
    """Configuration for a replication stream in Sling."""

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
    source_options: SlingSourceOptions | None = None
    """Options to further configure source."""
    target_options: SlingTargetOptions | None = None
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
    defaults: SlingReplicationStreamConfig
    """Default configuration for replication streams."""
    streams: dict[str, SlingReplicationStreamConfig]
    """Mapping of stream keys to their replication stream configurations."""
    env: dict[str, t.Any] | None = None
    """Environment variables to use for replication."""


# https://hamilton.dagworks.io/en/latest/
# https://github.com/DAGWorks-Inc/hamilton
class HamiltonAdapterConfig(_CDFConfigModel):
    """Configuration for the DLT adapter."""

    adapter: t.Literal["hamilton"] = "hamilton"

    inputs: dict[str, t.Any] = {}
    """The inputs to the Hamilton DAG."""
    scripts: Iterable[Path | str] = "main.py"
    """The script(s) to include in the Hamilton DAG."""


ExtractLoadConfig = (
    DltAdapterConfig | SingerAdapterConfig | SlingAdapterConfig | HamiltonAdapterConfig
)


class PytestAdapterConfig(_CDFConfigModel):
    adapter: t.Literal["pytest"] = "pytest"

    pytest_args: list[str] = []
    """Command-line style options to pass to pytest."""


class UnittestAdapterConfig(_CDFConfigModel):
    adapter: t.Literal["unittest"] = "unittest"

    test_pattern: str = "test*.py"
    """Pattern to match test files."""


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
    environment: str = "prod"
    """Environment name to use for SqlMesh."""


class JinjaSqlAdapterConfig(_CDFConfigModel):
    adapter: t.Literal["jinja_sql"] = "jinja_sql"

    connection_str: str = "duckdb:///:memory:"
    """SqlAlchemy connection string."""
    template_dir: str = "sql"
    """Path to the Jinja SQL templates."""
    variables: dict[str, t.Any] = {}
    """Variables to pass to the templates."""


TransformConfig = DbtTransformAdapterConfig | SqlMeshAdapterConfig | JinjaSqlAdapterConfig


class FileStateBackendConfig(_CDFConfigModel):
    adapter: t.Literal["file"] = "file"

    path: Path | str = Path("state.json")
    """Path to the file where the state will be stored."""
    buffered: bool = False
    """If True, writes are buffered and flushed to disk on exit."""
    dumper: t.Callable[[t.Any], str] | None = None
    """Optional custom function to serialize Python objects to strings."""
    loader: t.Callable[[str], t.Any] | None = None
    """Optional custom function to deserialize strings to Python objects."""

    @pydantic.field_validator("dumper", "loader", mode="before")
    def validate_callable(cls, value: t.Any):
        if isinstance(value, str):
            return resolve_entry_point(value)
        return value


class SqlAlchemyStateBackendConfig(_CDFConfigModel):
    adapter: t.Literal["sqlalchemy"] = "sqlalchemy"

    connection_str: str
    """Database URI to connect to."""
    table_name: str
    """Name of the table where the state will be stored."""
    schema_name: str | None = None
    """Name of the database schema."""
    dumper: t.Callable[[t.Any], str] | None = None
    """Optional custom function to serialize Python objects to strings."""
    loader: t.Callable[[str], t.Any] | None = None
    """Optional custom function to deserialize strings to Python objects."""

    @pydantic.field_validator("dumper", "loader", mode="before")
    def validate_callable(cls, value: t.Any):
        if isinstance(value, str):
            return resolve_entry_point(value)
        return value


StateBackendConfig = FileStateBackendConfig | SqlAlchemyStateBackendConfig


class PackageManifest(_CDFConfigModel):
    """Describes the outputs of a data package and provides metadata for interaction."""

    owner: str
    """Data owner or maintainer"""
    description: str | None = None
    """Package description"""
    tags: list[str] = []
    """List of tags for the package"""
    version: str = "0.1.0"
    """Semantic versioning"""
    dataset_names: list[str] = []
    """List of dataset names generated by the package"""
    transformed_dataset_names: list[str] = []
    """List of transformed dataset names generated by the package"""
    public_relation_patterns: list[str] = []
    """Patterns for tables that are exposed to the project"""
    pii_column_patterns: list[
        str
    ] = []  # Given a .schema method in the adapter, we can auto-tag tables
    """List of patterns for columns containing PII (Personally Identifiable Information)"""
    external_references: list[str] = []
    """Links to related resources, docs, or tickets"""
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

    manifest: PackageManifest
    """Manifest describing the data package outputs and metadata"""
    schedules: list[dict[str, str]] = []
    """List of schedules for the data package, for use with Scheduler adapter"""
    ingest: ExtractLoadConfig | None = None
    """Configuration for the extract and load adapter"""
    transform: TransformConfig | None = None
    """Configuration for the transform adapter"""
    test: TestConfig | None = None
    """Configuration for the test adapter"""


class ProjectConfig(_CDFConfigModel):
    """A project contains data packages"""

    name: str
    """Name of the project"""
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
    state_backend: StateBackendConfig | None = None
    """Configuration for the state backend."""
