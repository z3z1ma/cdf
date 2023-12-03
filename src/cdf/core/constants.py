"""Constants for the CDF module."""

# The main export symbols for CDF modules
CDF_PIPELINES = "__CDF_PIPELINES__"
CDF_PUBLISHERS = "__CDF_PUBLISHERS__"
CDF_SINKS = "__CDF_SINKS__"

# Core files
WORKSPACE_FILE = "cdf_workspace.toml"
CONFIG_FILE = "cdf_config.toml"
SECRETS_FILE = "cdf_secrets.toml"

# Flag file if using local flags for resource configuration
FLAG_FILE = "cdf_flags.json"

# The default workspace name, in a single-workspace layout it is always "default"
DEFAULT_WORKSPACE = "default"

# Paths relative to a root which constitute the layout of a CDF workspace
PIPELINES_PATH = "./pipelines"
TRANSFORMS_PATH = "./models"
PUBLISHERS_PATH = "./publishers"
METADATA_PATH = "./metadata"
SCRIPTS_PATH = "./scripts"

LOCKFILE_PATH = "./cdf.lock"

# The default layout of a CDF workspace
DIR_LAYOUT = (
    PIPELINES_PATH,
    TRANSFORMS_PATH,
    PUBLISHERS_PATH,
    SCRIPTS_PATH,
    METADATA_PATH,
    "./audits",
    "./macros",
    "./seeds",
    "./tests",
)
