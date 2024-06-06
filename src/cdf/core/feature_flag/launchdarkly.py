"""LaunchDarkly feature flag provider."""

import typing as t

from dlt.sources import DltSource

if t.TYPE_CHECKING:
    from cdf.core.project import LaunchDarklyFeatureFlagSettings


def apply_source(
    source: DltSource,
    /,
    *,
    settings: "LaunchDarklyFeatureFlagSettings",
    **kwargs: t.Any,
) -> DltSource:
    """Apply the feature flags to a dlt source."""
    _ = source, settings, kwargs
    raise NotImplementedError("LaunchDarkly feature flags are not yet supported")


__all__ = ["apply_source"]
