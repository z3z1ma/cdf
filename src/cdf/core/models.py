"""The models module defines pydantic models which concretely define the configuration schema for the CDF project."""

from __future__ import annotations

import typing as t
from pathlib import Path

import pydantic

import cdf.core.constants as c


class _CDFConfigModel(pydantic.BaseModel, arbitrary_types_allowed=True):
    pass


# https://dlthub.com/docs/intro
# https://github.com/dlt-hub/dlt
class DltAdapterConfig(_CDFConfigModel):
    adapter: t.Literal["dlt"] = "dlt"


# https://github.com/singer-io/getting-started
class SingerAdapterConfig(_CDFConfigModel):
    adapter: t.Literal["singer"] = "singer"


# https://slingdata.io/
# https://github.com/slingdata-io/sling-cli
class SlingAdapterConfig(_CDFConfigModel):
    adapter: t.Literal["sling"] = "sling"


ExtractLoadConfig = DltAdapterConfig | SingerAdapterConfig | SlingAdapterConfig


class PytestAdapterConfig(_CDFConfigModel):
    adapter: t.Literal["pytest"] = "pytest"


class UnittestAdapterConfig(_CDFConfigModel):
    adapter: t.Literal["unittest"] = "unittest"


class DbtTestAdapterConfig(_CDFConfigModel):
    adapter: t.Literal["dbt"] = "dbt"


TestConfig = PytestAdapterConfig | UnittestAdapterConfig | DbtTestAdapterConfig


class DbtTransformAdapterConfig(_CDFConfigModel):
    adapter: t.Literal["dbt"] = "dbt"


class SqlMeshAdapterConfig(_CDFConfigModel):
    adapter: t.Literal["sqlmesh"] = "sqlmesh"


class JinjaSqlAdapterConfig(_CDFConfigModel):
    adapter: t.Literal["jinja_sql"] = "jinja_sql"


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
