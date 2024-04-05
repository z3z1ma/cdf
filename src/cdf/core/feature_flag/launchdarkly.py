"""LaunchDarkly feature flag provider."""

import typing as t

from dlt.common.configuration import with_config


@with_config(sections=("feature_flags", "options"))
def create_launchdarkly_provider(**_: t.Any):
    raise NotImplementedError("LaunchDarkly feature flags are not yet supported")


__all__ = ["create_launchdarkly_provider"]
