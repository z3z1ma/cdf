"""No-op feature flag provider."""

import typing as t

import pydantic
from dlt.sources import DltSource

from cdf.core.feature_flag.base import BaseFlagProvider


class NoopFlagProvider(BaseFlagProvider, extra="allow"):
    """LaunchDarkly feature flag provider."""

    provider: t.Literal["noop"] = pydantic.Field(
        "noop", frozen=True, description="The feature flag provider."
    )

    def apply_source(self, source: DltSource) -> DltSource:
        return source


__all__ = ["NoopFlagProvider"]
