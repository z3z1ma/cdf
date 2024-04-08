"""LaunchDarkly feature flag provider."""

import typing as t

import pydantic
from dlt.sources import DltSource

from cdf.core.feature_flag.base import BaseFlagProvider


class LaunchDarklyFlagProvider(BaseFlagProvider, extra="allow"):
    """LaunchDarkly feature flag provider."""

    sdk_key: str = pydantic.Field(
        description="The LaunchDarkly SDK key used to connect to the LaunchDarkly service."
    )

    provider: t.Literal["launchdarkly"] = pydantic.Field(
        "launchdarkly", frozen=True, description="The feature flag provider."
    )

    def apply_source(self, source: DltSource) -> DltSource:
        raise NotImplementedError("LaunchDarkly feature flags are not yet supported")


__all__ = ["LaunchDarklyFlagProvider"]
