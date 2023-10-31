"""Constants for the CDF module."""
import re

CDF_SOURCE = "__CDF_SOURCE__"

DEST_CRED_PAT = re.compile(r"^CDF_(?P<dest_name>.+)__(?P<engine_name>.+)__(?P<key>.+)$")
NATIVE_DEST_CRED_PAT = re.compile(r"^CDF_(?P<dest_name>.+)__(?P<engine_name>.+)$")
