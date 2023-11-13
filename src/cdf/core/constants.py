"""Constants for the CDF module."""
import re
from collections import namedtuple

CDF_SOURCE = "__CDF_SOURCE__"
CDF_WORKSPACE_FILE = "cdf_workspace.toml"
CDF_CONFIG_FILE = "cdf_config.toml"
CDF_SECRETS_FILE = "cdf_secrets.toml"

DEST_ENGINE_PAT = re.compile(r"^CDF__(?P<dest_name>.+)__ENGINE$")
DEST_CRED_PAT = re.compile(r"^CDF__(?P<dest_name>.+)__CREDENTIALS__(?P<key>.+)$")
DEST_NATIVECRED_PAT = re.compile(r"^CDF__(?P<dest_name>.+)__CREDENTIALS$")

CDF_FLAG_FILES = [
    "cdf.json",
    ".cdf.json",
    "cdf_flags.json",
    ".cdf_flags.json",
    "flags.json",
]

COMPONENT_PATHS = namedtuple(
    "COMPONENT_PATHS", ["sources", "transforms", "publishers"]
)("sources", "transforms", "publishers")

DEFAULT_WORKSPACE = "default"

SOURCES_PATH = "./sources"
TRANSFORMS_PATH = "./transforms"
PUBLISHERS_PATH = "./publishers"
