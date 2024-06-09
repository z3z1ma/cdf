"""No-op feature flag provider."""

import typing as t

from cdf.core.feature_flag.base import AbstractFeatureFlagAdapter


class NoopFeatureFlagAdapter(AbstractFeatureFlagAdapter):
    """A feature flag adapter that does nothing."""

    def get(self, feature_name: str) -> bool:
        return True

    def save(self, feature_name: str, flag: bool) -> None:
        pass

    def get_all_feature_names(self) -> t.List[str]:
        return []


__all__ = ["NoopFeatureFlagAdapter"]
