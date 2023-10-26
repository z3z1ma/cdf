"""The source class for continuous data flow sources."""
import typing as t
from functools import partial

import dlt
from dlt.common.schema import Schema  # type: ignore
from dlt.extract.source import DltResource, DltSource

from cdf.core.registry import register_source


class ContinuousDataFlowSource(DltSource):
    """A source class for continuous data flow sources."""

    def __init__(
        self,
        name: str,
        section: str,
        schema: Schema,
        resources: t.Sequence[DltResource] | None = None,
    ) -> None:
        super().__init__(name, section, schema, resources or [])
        register_source(source=self)


source = partial(dlt.source, _impl_cls=ContinuousDataFlowSource)  # type: ignore
"""A wrapper around dlt.source that registers the source class with the registry."""

resource = dlt.resource  # type: ignore
"""A wrapper around dlt.resource."""

__all__ = ["ContinuousDataFlowSource", "source", "resource"]
