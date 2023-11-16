"""Constants for the CDF module."""
import re

# The main export symbol for CDF source modules
CDF_SOURCE = "__CDF_SOURCE__"

# Core files
WORKSPACE_FILE = "cdf_workspace.toml"
CONFIG_FILE = "cdf_config.toml"
SECRETS_FILE = "cdf_secrets.toml"

# Ancillary flag files if using local flags for resource configuration
FLAG_FILES = [
    "cdf.json",
    ".cdf.json",
    "cdf_flags.json",
    ".cdf_flags.json",
    "flags.json",
]

# The default workspace name, in a single-workspace layout it is always "default"
DEFAULT_WORKSPACE = "default"

# Paths relative to a root which constitute the layout of a CDF workspace
SOURCES_PATH = "./sources"
TRANSFORMS_PATH = "./transforms"
PUBLISHERS_PATH = "./publishers"
LOCKFILE_PATH = "./cdf.lock"

# Destination resolution constants, likely to be factored out
DEST_ENGINE_PAT = re.compile(r"^CDF__(?P<dest_name>.+)__ENGINE$")
DEST_CRED_PAT = re.compile(r"^CDF__(?P<dest_name>.+)__CREDENTIALS__(?P<key>.+)$")
DEST_NATIVECRED_PAT = re.compile(r"^CDF__(?P<dest_name>.+)__CREDENTIALS$")
