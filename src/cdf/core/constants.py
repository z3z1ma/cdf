"""Constants for the CDF module."""
import re

CDF_SOURCE = "__CDF_SOURCE__"

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

CDF_CONFIG_FILE = "cdf_config.toml"
CDF_SECRETS_FILE = "cdf_secrets.toml"

COMPONENT_PATHS = ["./sources", "./transforms", "./publishers"]
