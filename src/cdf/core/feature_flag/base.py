import abc
import typing as t

import pydantic

if t.TYPE_CHECKING:
    from dlt.sources import DltSource


class BaseFlagProvider(pydantic.BaseModel, abc.ABC):
    provider: str

    @abc.abstractmethod
    def apply_source(self, source: "DltSource") -> "DltSource":
        """Apply the feature flags to a dlt source."""
