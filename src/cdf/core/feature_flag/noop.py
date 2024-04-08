"""No-op feature flag provider."""

import typing as t

from dlt.sources import DltSource

from cdf.core.feature_flag.base import BaseFlagProvider


class NoopFlagProvider(BaseFlagProvider, extra="allow"):
    """LaunchDarkly feature flag provider."""

    provider: t.Literal["noop"] = "noop"

    def apply_source(self, source: DltSource) -> DltSource:
        return source


__all__ = ["NoopFlagProvider"]
