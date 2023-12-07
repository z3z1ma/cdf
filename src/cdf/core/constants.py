"""Constants for the CDF module."""

# The main export symbols for CDF modules
CDF_PIPELINES = "__CDF_PIPELINES__"
CDF_PUBLISHERS = "__CDF_PUBLISHERS__"
CDF_SINKS = "__CDF_SINKS__"

# Core files
WORKSPACE_FILE = "cdf_workspace.toml"
CONFIG_FILE = "cdf_config.toml"

# Flag file if using local flags for resource configuration
FLAG_FILE = "cdf_flags.json"

# The file containing the sinks for a workspace
SINKS_FILE = "cdf_sinks.py"

# Default requirements file
REQUIREMENTS_FILE = "requirements.txt"

# The default workspace name, in a single-workspace layout it is always "default"
DEFAULT_WORKSPACE = "default"

# Paths relative to a root which constitute the layout of a CDF workspace
PIPELINES = "pipelines"
TRANSFORMS = "models"
PUBLISHERS = "publishers"
METADATA = "metadata"
SCRIPTS = "scripts"

# Default virtual environment path
VENV = ".venv"

# The default lockfile path
LOCKFILE = "cdf.lock"

# The default layout of a CDF workspace
DIR_LAYOUT = (
    PIPELINES,
    TRANSFORMS,
    PUBLISHERS,
    SCRIPTS,
    METADATA,
    "audits",
    "macros",
    "seeds",
    "tests",
)
