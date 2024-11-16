"""No-op feature flag provider."""

import typing as t

from cdf.integrations.feature_flag.base import AbstractFeatureFlagAdapter


class NoopFeatureFlagAdapter(AbstractFeatureFlagAdapter):
    """A feature flag adapter that does nothing."""

    def __init__(self, **kwargs: t.Any) -> None:
        """Initialize the adapter."""
        pass

    def get(self, feature_name: str) -> bool:  # type: ignore
        return True

    def save(self, feature_name: str, flag: bool) -> None:
        pass

    def get_all_feature_names(self) -> t.List[str]:
        return []


__all__ = ["NoopFeatureFlagAdapter"]
