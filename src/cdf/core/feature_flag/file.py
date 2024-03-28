"""File-based feature flag provider."""

import json
import typing as t
from threading import Lock

import dlt
from dlt.common.configuration import with_config

if t.TYPE_CHECKING:
    from dlt.sources import DltSource

    from cdf.core.feature_flag import SupportsFFs

WLock = Lock()


@with_config(sections=("feature_flags", "options"))
def create_file_provider(
    path: str = dlt.config.value,
) -> "SupportsFFs":
    def _processor(source: "DltSource") -> "DltSource":
        with open(path) as file:
            flags = json.load(file)

        source_name = source.name
        for resource_name, resource in source.selected_resources.items():
            key = f"{source_name}.{resource_name}"
            resource.selected = flags.setdefault(key, False)

        with WLock, open(path, "w") as file:
            json.dump(flags, file, indent=2)

        return source

    return _processor
