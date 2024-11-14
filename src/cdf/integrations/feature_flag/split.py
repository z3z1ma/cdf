"""Split feature flag provider."""

import typing as t

from cdf.integrations.feature_flag.base import AbstractFeatureFlagAdapter


class SplitFeatureFlagAdapter(AbstractFeatureFlagAdapter):
    """A feature flag adapter that uses Split."""

    def __init__(self, sdk_key: str, **kwargs: t.Any) -> None:
        """Initialize the Split feature flags.

        Args:
            sdk_key: The SDK key to use for Split.
        """
        self.sdk_key = sdk_key

    def __repr__(self) -> str:
        return f"{type(self).__name__}(sdk_key={self.sdk_key!r})"

    def __str__(self) -> str:
        return self.sdk_key

    def get(self, feature_name: str) -> bool:  # type: ignore
        raise NotImplementedError("This provider is not yet implemented")

    def save(self, feature_name: str, flag: bool) -> None:
        raise NotImplementedError("This provider is not yet implemented")

    def get_all_feature_names(self) -> t.List[str]:
        raise NotImplementedError("This provider is not yet implemented")


__all__ = ["SplitFeatureFlagAdapter"]
