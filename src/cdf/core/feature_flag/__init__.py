"""Feature flag providers."""

import typing as t

import pydantic
from dlt.common.configuration import with_config

from cdf.core.feature_flag.file import FileFlagProvider
from cdf.core.feature_flag.harness import HarnessFlagProvider
from cdf.core.feature_flag.launchdarkly import LaunchDarklyFlagProvider
from cdf.core.feature_flag.noop import NoopFlagProvider

FlagProvider = t.Union[
    FileFlagProvider,
    HarnessFlagProvider,
    LaunchDarklyFlagProvider,
    NoopFlagProvider,
]

_FlagProvider: pydantic.TypeAdapter[FlagProvider] = pydantic.TypeAdapter(FlagProvider)


def _ensure_dict(o: t.Any) -> t.Dict[str, t.Any]:
    """Unwraps dynaconf config objects to dict."""
    if isinstance(o, dict):
        return o
    return o.to_dict()


@with_config(sections=("feature_flags",))
def load_feature_flag_provider(
    provider: t.Literal["file", "harness", "launchdarkly", "noop"] = "noop",
    options: t.Optional[t.Dict[str, t.Any]] = None,
) -> FlagProvider:
    options = _ensure_dict(options or {})
    options["provider"] = provider
    return _FlagProvider.validate_python(options)


__all__ = [
    "load_feature_flag_provider",
    "FlagProvider",
    "FileFlagProvider",
    "HarnessFlagProvider",
    "LaunchDarklyFlagProvider",
    "NoopFlagProvider",
]
