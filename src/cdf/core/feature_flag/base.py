import abc
import typing as t
from enum import Enum, auto

if t.TYPE_CHECKING:
    from dlt.sources import DltSource


class FlagAdapterResponse(Enum):
    """Feature flag response.

    This enum is used to represent the state of a feature flag. It is similar
    to a boolean but with an extra state for when the flag is not found.
    """

    ENABLED = auto()
    """The feature flag is enabled."""
    DISABLED = auto()
    """The feature flag is disabled."""
    NOT_FOUND = auto()
    """The feature flag is not found."""

    def __bool__(self) -> bool:
        """Return True if the flag is enabled and False otherwise."""
        return self is FlagAdapterResponse.ENABLED

    to_bool = __bool__

    @classmethod
    def from_bool(cls, flag: bool) -> "FlagAdapterResponse":
        """Convert a boolean to a flag response."""
        return cls.ENABLED if flag else cls.DISABLED


class AbstractFeatureFlagAdapter(abc.ABC):
    """Abstract feature flag adapter."""

    def __init__(self, **kwargs: t.Any) -> None:
        """Initialize the adapter."""
        pass

    @abc.abstractmethod
    def get(self, feature_name: str) -> FlagAdapterResponse:
        """Get the feature flag."""
        pass

    def __getitem__(self, feature_name: str) -> FlagAdapterResponse:
        """Get the feature flag."""
        return self.get(feature_name)

    def get_many(self, feature_names: t.List[str]) -> t.Dict[str, FlagAdapterResponse]:
        """Get many feature flags.

        Implementations should override this method if they can optimize it. The default
        will call get in a loop.
        """
        return {feature_name: self.get(feature_name) for feature_name in feature_names}

    @abc.abstractmethod
    def save(self, feature_name: str, flag: bool) -> None:
        """Save the feature flag."""
        pass

    def __setitem__(self, feature_name: str, flag: bool) -> None:
        """Save the feature flag."""
        self.save(feature_name, flag)

    def save_many(self, flags: t.Dict[str, bool]) -> None:
        """Save many feature flags.

        Implementations should override this method if they can optimize it. The default
        will call save in a loop.
        """
        for feature_name, flag in flags.items():
            self.save(feature_name, flag)

    @abc.abstractmethod
    def get_all_feature_names(self) -> t.List[str]:
        """Get all feature names."""
        pass

    def keys(self) -> t.List[str]:
        """Get all feature names."""
        return self.get_all_feature_names()

    def __iter__(self) -> t.Iterator[str]:
        """Iterate over the feature names."""
        return iter(self.get_all_feature_names())

    def __contains__(self, feature_name: str) -> bool:
        """Check if a feature flag exists."""
        return self.get(feature_name) is not FlagAdapterResponse.NOT_FOUND

    def __len__(self) -> int:
        """Get the number of feature flags."""
        return len(self.get_all_feature_names())

    def delete(self, feature_name: str) -> None:
        """Delete a feature flag.

        By default, this will disable the flag but implementations can override this method
        to delete the flag.
        """
        self.save(feature_name, False)

    __delitem__ = delete

    def delete_many(self, feature_names: t.List[str]) -> None:
        """Delete many feature flags."""
        self.save_many({feature_name: False for feature_name in feature_names})

    def apply_source(self, source: "DltSource") -> "DltSource":
        """Apply the feature flags to a dlt source.

        Args:
            source: The source to apply the feature flags to.

        Returns:
            The source with the feature flags applied.
        """
        new = {}
        source_name = source.name
        for resource_name, resource in source.selected_resources.items():
            resp = self.get(k := f"{source_name}.{resource_name}")
            resource.selected = bool(resp)
            if resp is FlagAdapterResponse.NOT_FOUND:
                new[k] = False
        if new:
            self.save_many(new)

        return source
