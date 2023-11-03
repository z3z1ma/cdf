"""The source class for continuous data flow sources."""
import typing as t
from dataclasses import dataclass
from functools import partial

import dlt
from dlt.common.schema import Schema
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
        self.flags = {}

    def setup(
        self, alias: str | None = None, raise_on_no_resources: bool = False
    ) -> None:
        import cdf.core.feature_flags as ff

        if alias:
            self.name = alias
        for name, resource in self.resources.items():
            component_id = f"{self.base_component_id}:{name}"
            flag = ff.get_component_ff(component_id)
            resource.selected = flag[component_id]
            self.flags.update(flag)
        if raise_on_no_resources and not self.resources.selected:
            raise ValueError(f"No resources selected for source {self.name}")

    @property
    def base_component_id(self) -> str:
        return f"source:{self.name}"

    def component_id(self, resource_name: str) -> str:
        return f"{self.base_component_id}:{resource_name}"


LazySource = t.Callable[[], CDFSource]


@dataclass
class CDFSourceMeta:
    """A class to hold metadata about a source."""

    deferred_fn: LazySource
    version: int = 1
    owners: t.Sequence[str] = ()
    description: str = ""
    tags: t.Sequence[str] = ()


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
