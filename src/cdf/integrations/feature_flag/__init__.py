"""Feature flag providers implement a uniform interface and are wrapped by an adapter.

The adapter is responsible for loading the correct provider and applying the feature flags within
various contexts in cdf. This allows for a clean separation of concerns and makes it easy to
implement new feature flag providers in the future.
"""

import typing as t

import dlt
from dlt.common.configuration import with_config

from cdf.integrations.feature_flag.base import AbstractFeatureFlagAdapter
from cdf.integrations.feature_flag.file import FilesystemFeatureFlagAdapter
from cdf.integrations.feature_flag.harness import HarnessFeatureFlagAdapter
from cdf.integrations.feature_flag.launchdarkly import \
    LaunchDarklyFeatureFlagAdapter
from cdf.integrations.feature_flag.noop import NoopFeatureFlagAdapter
from cdf.integrations.feature_flag.split import SplitFeatureFlagAdapter
from cdf.types import M

ADAPTERS: t.Dict[str, t.Type[AbstractFeatureFlagAdapter]] = {
    "filesystem": FilesystemFeatureFlagAdapter,
    "harness": HarnessFeatureFlagAdapter,
    "launchdarkly": LaunchDarklyFeatureFlagAdapter,
    "split": SplitFeatureFlagAdapter,
    "noop": NoopFeatureFlagAdapter,
}
"""Feature flag provider adapters classes by name."""


@with_config(sections=("feature_flags",))
def get_feature_flag_adapter_cls(
    provider: str = dlt.config.value,
) -> M.Result[t.Type[AbstractFeatureFlagAdapter], Exception]:
    """Get a feature flag adapter by name.

    Args:
        provider: The name of the feature flag adapter.
        options: The configuration for the feature flag adapter.

    Returns:
        The feature flag adapter.
    """
    try:
        if provider not in ADAPTERS:
            raise KeyError(
                f"Unknown provider: {provider}. Available providers: {', '.join(ADAPTERS.keys())}"
            )
        return M.ok(ADAPTERS[provider])
    except KeyError as e:
        # Notify available providers
        return M.error(e)
    except Exception as e:
        return M.error(e)


__all__ = [
    "ADAPTERS",
    "AbstractFeatureFlagAdapter",
    "FilesystemFeatureFlagAdapter",
    "HarnessFeatureFlagAdapter",
    "LaunchDarklyFeatureFlagAdapter",
    "NoopFeatureFlagAdapter",
    "SplitFeatureFlagAdapter",
    "get_feature_flag_adapter_cls",
]
