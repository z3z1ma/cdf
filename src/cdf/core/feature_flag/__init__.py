"""Feature flag providers implement a uniform interface and are wrapped by an adapter.

The adapter is responsible for loading the correct provider and applying the feature flags within
various contexts in cdf. This allows for a clean separation of concerns and makes it easy to
implement new feature flag providers in the future.
"""

import typing as t

from dlt.common.configuration import with_config

from cdf.core.feature_flag import file, harness, launchdarkly, noop
from cdf.core.filesystem import FilesystemAdapter
from cdf.types import M

if t.TYPE_CHECKING:
    from dlt.sources import DltSource

    from cdf.core.project import FeatureFlagProviderType, FeatureFlagSettings


class _AdapterProtocol(t.Protocol):
    """Feature flag provider adapter protocol."""

    def apply_source(
        self,
        source: "DltSource",
        /,
        *,
        settings: t.Any,
        **kwargs: t.Any,
    ) -> "DltSource": ...


ADAPTERS: t.Dict[str, _AdapterProtocol] = {
    "filesystem": file,
    "harness": harness,
    "launchdarkly": launchdarkly,
    "noop": noop,
}
"""Feature flag provider adapters."""


class FeatureFlagAdapter:
    """An adapter for feature flag providers."""

    @with_config(sections=("feature_flags",))
    def __init__(
        self, settings: "FeatureFlagSettings", /, filesystem: FilesystemAdapter
    ) -> None:
        if settings.provider not in ADAPTERS:
            raise ValueError(f"Unknown feature flag provider: {settings.provider}")
        self.settings = settings
        self._filesystem = filesystem

    @property
    def provider(self) -> "FeatureFlagProviderType":
        return self.settings.provider

    def apply_source(self, source: "DltSource", **kwargs: t.Any) -> "DltSource":
        """Apply the feature flags to a dlt source."""
        return ADAPTERS[self.provider].apply_source(
            source, settings=self.settings, filesystem=self._filesystem, **kwargs
        )


def get_feature_flag_adapter(
    settings: "FeatureFlagSettings", /, filesystem: FilesystemAdapter
) -> M.Result[FeatureFlagAdapter, Exception]:
    """Get a feature flag adapter from settings.

    Args:
        settings: The feature flag settings.
        filesystem: The filesystem adapter.

    Returns:
        M.Result[FeatureFlagAdapter, Exception]: The feature flag adapter or an error.
    """
    try:
        return M.ok(FeatureFlagAdapter(settings, filesystem))
    except Exception as e:
        return M.error(e)


__all__ = ["get_feature_flag_adapter", "FeatureFlagAdapter"]
