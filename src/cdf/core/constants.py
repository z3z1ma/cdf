"""Constants for the CDF module."""
import re

CDF_SOURCE = "__CDF_SOURCE__"

DEST_CRED_PAT = re.compile(r"^CDF_(?P<dest_name>.+)__(?P<engine_name>.+)__(?P<key>.+)$")
NATIVE_DEST_CRED_PAT = re.compile(r"^CDF_(?P<dest_name>.+)__(?P<engine_name>.+)$")

CDF_FLAG_FILES = [
    ".cdf.json",
    "cdf.json",
    "flags.json",
    ".cdf_flags.json",
    "cdf_flags.json",
]

COMPONENT_PATHS = ["./sources", "./transforms", "./publishers"]
