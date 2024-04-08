"""File-based feature flag provider."""

import json
import typing as t
from threading import Lock

import fsspec
import pydantic

import cdf.core.logger as logger
from cdf.core.feature_flag.base import BaseFlagProvider

if t.TYPE_CHECKING:
    from dlt.sources import DltSource


class FileFlagProvider(BaseFlagProvider, extra="allow", arbitrary_types_allowed=True):
    path: str = pydantic.Field(
        description="The path to the file where the feature flags are stored in the configured filesystem."
    )
    storage: fsspec.AbstractFileSystem = pydantic.Field(
        default=fsspec.filesystem("file"),
        description="This leverages the configured filesystem and can be used to store the flags in S3, GCS, etc. It should not be set directly.",
        exclude=True,
    )

    provider: t.Literal["file"] = pydantic.Field(
        "file", frozen=True, description="The feature flag provider."
    )

    _lock: Lock = pydantic.PrivateAttr(default_factory=Lock)

    def apply_source(self, source: "DltSource") -> "DltSource":
        """Apply the feature flags to a dlt source."""
        logger.info("Reading feature flags from %s", self.path)
        if not self.storage.exists(self.path):
            flags = {}
        else:
            with self.storage.open(self.path) as file:
                flags = json.load(file)
        source_name = source.name
        for resource_name, resource in source.selected_resources.items():
            key = f"{source_name}.{resource_name}"
            resource.selected = flags.setdefault(key, False)
        with self._lock, self.storage.open(self.path, "w") as file:
            json.dump(flags, file, indent=2)
        return source


__all__ = ["FileFlagProvider"]
