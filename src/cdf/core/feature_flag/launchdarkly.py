"""LaunchDarkly feature flag provider."""

import typing as t

from dlt.sources import DltSource

from cdf.core.feature_flag.base import BaseFlagProvider


class LaunchDarklyFlagProvider(BaseFlagProvider, extra="allow"):
    """LaunchDarkly feature flag provider."""

    sdk_key: str

    provider: t.Literal["launchdarkly"] = "launchdarkly"

    def apply_source(self, source: DltSource) -> DltSource:
        raise NotImplementedError("LaunchDarkly feature flags are not yet supported")


__all__ = ["LaunchDarklyFlagProvider"]
