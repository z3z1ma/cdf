"""Feature flag providers."""

import typing as t

import fsspec
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


class NoopProviderOptions(t.TypedDict): ...


@t.overload
def load_feature_flag_provider(
    provider: t.Literal["noop"] = "noop",
    options: t.Optional[NoopProviderOptions] = None,
) -> SupportsFFs: ...


class FileProviderOptions(t.TypedDict):
    path: str
    fs: fsspec.AbstractFileSystem


@t.overload
def load_feature_flag_provider(
    provider: t.Literal["file"] = "file",
    options: t.Optional[FileProviderOptions] = None,
) -> SupportsFFs: ...


class HarnessProviderOptions(t.TypedDict):
    api_key: str
    sdk_key: str
    account: str
    organization: str
    project: str


@t.overload
def load_feature_flag_provider(
    provider: t.Literal["harness"] = "harness",
    options: t.Optional[HarnessProviderOptions] = None,
) -> SupportsFFs: ...


class LaunchDarklyProviderOptions(t.TypedDict):
    sdk_key: str


@t.overload
def load_feature_flag_provider(
    provider: t.Literal["launchdarkly"] = "launchdarkly",
    options: t.Optional[LaunchDarklyProviderOptions] = None,
) -> SupportsFFs: ...


@with_config(sections=("feature_flags",))
def load_feature_flag_provider(
    provider: t.Literal["file", "harness", "launchdarkly", "noop"] = "noop",
    options: t.Optional[
        t.Union[
            NoopProviderOptions,
            FileProviderOptions,
            HarnessProviderOptions,
            LaunchDarklyProviderOptions,
        ]
    ] = None,
) -> SupportsFFs:
    opts = t.cast(dict, options or {})
    if provider == "file":
        logger.info("Using file-based feature flags")
        return create_file_provider(**opts)
    if provider == "harness":
        logger.info("Using Harness feature flags")
        return create_harness_provider(**opts)
    if provider == "launchdarkly":
        logger.info("Using LaunchDarkly feature flags")
        return create_launchdarkly_provider(**opts)
    if provider is None or provider == "noop":
        logger.info("No feature flag provider configured")
        return create_noop_provider(**opts)
    raise ValueError(f"Unknown feature flag provider: {provider}")


__all__ = [
    "SupportsFFs",
    "create_noop_provider",
    "create_file_provider",
    "create_harness_provider",
    "create_launchdarkly_provider",
    "load_feature_flag_provider",
]
