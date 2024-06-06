"""File-based feature flag provider."""

import json
import typing as t
from collections import defaultdict
from threading import Lock

import cdf.core.logger as logger

if t.TYPE_CHECKING:
    from dlt.sources import DltSource

    from cdf.core.filesystem import FilesystemAdapter
    from cdf.core.project import FilesystemFeatureFlagSettings


LOCK = defaultdict(Lock)


def apply_source(
    source: "DltSource",
    /,
    *,
    settings: "FilesystemFeatureFlagSettings",
    filesystem: "FilesystemAdapter",
    **kwargs: t.Any,
) -> "DltSource":
    """Apply the feature flags to a dlt source."""
    _ = kwargs
    logger.info("Reading feature flags from %s", settings.filename)
    if not filesystem.exists(settings.filename):
        flags = {}
    else:
        with filesystem.open(settings.filename) as file:
            flags = json.load(file)
    source_name = source.name
    for resource_name, resource in source.selected_resources.items():
        key = f"{source_name}.{resource_name}"
        resource.selected = flags.setdefault(key, False)
    with LOCK[settings], filesystem.open(settings.filename, "w") as file:
        json.dump(flags, file, indent=2)
    return source


__all__ = ["apply_source"]
