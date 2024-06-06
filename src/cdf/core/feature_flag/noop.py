"""No-op feature flag provider."""

import typing as t

from dlt.sources import DltSource

if t.TYPE_CHECKING:
    from cdf.core.project import NoopFeatureFlagSettings


def apply_source(
    source: DltSource, /, *, settings: "NoopFeatureFlagSettings", **kwargs: t.Any
) -> DltSource:
    """Apply the feature flags to a dlt source."""
    _ = settings, kwargs
    return source


__all__ = ["apply_source"]
