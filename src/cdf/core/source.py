"""The source class for continuous data flow sources."""
import typing as t
from contextlib import nullcontext
from dataclasses import dataclass, field
from functools import partial

import dlt
from dlt.common.schema import Schema
from dlt.common.typing import TDataItem
from dlt.extract.source import DltResource, DltSource

from cdf.core.registry import register_source


class CDFSource(DltSource):
    """A source class for continuous data flow sources."""

    def __init__(
        self,
        name: str,
        section: str,
        schema: Schema,
        resources: t.Sequence[DltResource] | None = None,
    ) -> None:
        super().__init__(name, section, schema, resources or [])
        register_source(source=self)  # TODO: no value in this, remove


LazySource = t.Callable[[], CDFSource]


@dataclass
class CDFSourceMeta:
    """A class to hold metadata about a source."""

    deferred_fn: LazySource
    version: int = 1
    owners: t.Sequence[str] = ()
    description: str = ""
    tags: t.Sequence[str] = ()
    cron: str | None = None
    metrics: t.Dict[str, t.Callable[[TDataItem, float | int], float | int]] = field(
        default_factory=dict
    )


source = partial(dlt.source, _impl_cls=CDFSource)
"""A wrapper around dlt.source that registers the source class with the registry."""

resource = dlt.resource  # type: ignore
"""A wrapper around dlt.resource. Reserving this for future use."""

__all__ = [
    "CDFSource",
    "CDFSourceMeta",
    "LazySource",
    "source",
    "resource",
]
