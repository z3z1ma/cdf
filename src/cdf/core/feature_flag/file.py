"""File-based feature flag provider."""

import json
import typing as t
from collections import defaultdict
from threading import Lock

import dlt
import fsspec
from dlt.common.configuration import with_config

import cdf.core.logger as logger
from cdf.core.feature_flag.base import AbstractFeatureFlagAdapter, FlagAdapterResponse
from cdf.core.state import with_audit


class FilesystemFeatureFlagAdapter(AbstractFeatureFlagAdapter):
    """A feature flag adapter that uses the filesystem."""

    _LOCK = defaultdict(Lock)

    @with_config(sections=("feature_flags",))
    def __init__(
        self,
        filesystem: fsspec.AbstractFileSystem,
        filename: str = dlt.config.value,
        **kwargs: t.Any,
    ) -> None:
        """Initialize the filesystem feature flags.

        Args:
            filesystem: The filesystem to use.
            filename: The filename to use for the feature flags.
        """
        self.filename = filename
        self.filesystem = filesystem
        self.__flags: t.Optional[t.Dict[str, FlagAdapterResponse]] = None

    def __repr__(self) -> str:
        return f"{type(self).__name__}(filename={self.filename!r})"

    def __str__(self) -> str:
        return self.filename

    @with_audit(
        "feature_flag_filesystem_read", lambda self: {"filename": self.filename}
    )
    def _read(self) -> t.Dict[str, FlagAdapterResponse]:
        """Read the feature flags from the filesystem."""
        logger.info("Reading feature flags from %s", self.filename)
        if not self.filesystem.exists(self.filename):
            flags = {}
        else:
            with self.filesystem.open(self.filename) as file:
                flags = json.load(file)
        return {k: FlagAdapterResponse.from_bool(v) for k, v in flags.items()}

    def _commit(self) -> None:
        """Commit the feature flags to the filesystem."""
        logger.info("Committing feature flags to %s", self.filename)
        with (
            self._LOCK[self.filename],
            self.filesystem.open(self.filename, "w") as file,
        ):
            json.dump({k: v.to_bool() for k, v in self._flags.items()}, file, indent=2)

    @property
    def _flags(self) -> t.Dict[str, FlagAdapterResponse]:
        """Get the feature flags."""
        if self.__flags is None:
            self.__flags = self._read()
        return self.__flags

    def get(self, feature_name: str) -> FlagAdapterResponse:
        """Get a feature flag.

        Args:
            feature_name: The name of the feature flag.

        Returns:
            The feature flag.
        """
        return self._flags.get(feature_name, FlagAdapterResponse.NOT_FOUND)

    def get_all_feature_names(self) -> t.List[str]:
        """Get all feature flag names.

        Returns:
            The feature flag names.
        """
        return list(self._flags.keys())

    def save(self, feature_name: str, flag: bool) -> None:
        """Save a feature flag.

        Args:
            feature_name: The name of the feature flag.
            flag: The value of the feature flag.
        """
        self._flags[feature_name] = FlagAdapterResponse.from_bool(flag)
        self._commit()

    def save_many(self, flags: t.Dict[str, bool]) -> None:
        """Save multiple feature flags.

        Args:
            flags: The feature flags to save.
        """
        self._flags.update(
            {k: FlagAdapterResponse.from_bool(v) for k, v in flags.items()}
        )
        self._commit()


__all__ = ["FilesystemFeatureFlagAdapter"]
