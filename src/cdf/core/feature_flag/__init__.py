"""Feature flag providers."""

import typing as t

from dlt.common.configuration import with_config

import cdf.core.logger as logger
from cdf.core.feature_flag.file import create_file_provider
from cdf.core.feature_flag.harness import create_harness_provider
from cdf.core.feature_flag.launchdarkly import create_launchdarkly_provider

if t.TYPE_CHECKING:
    from dlt.sources import DltSource


# The general interface for a feature flag provider
# TODO: we should decouple the protocol from dlt sources
class SupportsFFs(t.Protocol):
    def __call__(self, source: "DltSource") -> "DltSource": ...


def create_noop_provider() -> SupportsFFs:
    def _processor(source: "DltSource") -> "DltSource":
        return source

    return _processor


@with_config(sections=("feature_flags",))
def create_provider(provider: t.Optional[str] = None, **options: t.Any) -> SupportsFFs:
    if provider == "file":
        logger.info("Using file-based feature flags")
        return create_file_provider(**options)
    if provider == "harness":
        logger.info("Using Harness feature flags")
        return create_harness_provider(**options)
    if provider == "launchdarkly":
        logger.info("Using LaunchDarkly feature flags")
        return create_launchdarkly_provider(**options)
    if provider is None or provider == "noop":
        logger.info("No feature flag provider configured")
        return create_noop_provider(**options)
    raise ValueError(f"Unknown feature flag provider: {provider}")
