"""Constants for the CDF module."""
import typing as t

TComponents = t.Literal[
    "pipelines",
    "models",
    "publishers",
    "scripts",
    "sinks",
    "pipelines.metrics",
    "pipelines.filters",
    "notebooks",
]
"""CDF component types"""

COMPONENTS = t.get_args(TComponents)
"""The various component types in CDF (which correspond to directories in a workspace)"""

PIPELINES = "pipelines"
MODELS = "models"
PUBLISHERS = "publishers"
SCRIPTS = "scripts"
SINKS = "sinks"
METRICS = "pipelines.metrics"
FILTERS = "pipelines.filters"
METADATA = "metadata"
NOTEBOOKS = "notebooks"

SPECS = "spec"
"""A namespace used in a cdf configuration TOML to declare specifications"""

STAGING = "staging"
"""A namespace used in a cdf configuration TOML to declare staging model generation"""

TRANSFORM_SPEC = "transform"
"""A namespace used in a cdf configuration TOML to declare SQLMesh config kwargs"""

DEFAULT_WORKSPACE = "default"
"""The default workspace name, in a single-workspace layout it is always 'default'"""

PROJECT_FILE = "cdf_project.toml"
"""The top-level file used in a multi-workspace layout to declare workspaces via relative paths"""

CONFIG_FILE = "cdf_config.toml"
"""The primary configuration file for a CDF workspace"""

LOCK_FILE = "cdf.lock"
"""A file which stores auto-generated information which should be committed to git such as hashes"""

FLAG_FILE = "cdf_flags.json"
"""Flag file if using local flags for resource configuration"""

SQLMESH_METADATA_FILE = "_cdf_unmanaged.yaml"
"""File for sqlmesh external models which are not managed/ingested by CDF"""

DIR_LAYOUT = (
    PIPELINES,
    MODELS,
    PUBLISHERS,
    SINKS,
    SCRIPTS,
    NOTEBOOKS,
    METADATA,
    METRICS,
    FILTERS,
    "audits",
    "macros",
    "seeds",
    "tests",
)
"""The default layout of a CDF workspace"""

INTERNAL_SCHEMA = "cdf_internal"
"""An internal schema used to track metadata"""

LOAD_INFO_TABLE = "load_info"
"""Table which tracks load info"""

EXC_INFO_TABLE = "exc_info"
"""Table which track failures in pipeline execution"""

METRIC_INFO_TABLE = "_cdf_metrics"
"""Table which tracks runtime metrics"""

DEFAULT_CONFIG = {"ff": {"provider": "local"}}
"""Default configuration for a CDF workspace"""

SOURCE_CONTAINER = "__sources__"
"""A key used to capture sources during runtime"""
