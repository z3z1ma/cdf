"""The source class for continuous data flow sources."""
import typing as t
from functools import partial

import dlt
from dlt.common.schema import Schema  # type: ignore
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

    def setup(self, alias: str | None = None) -> None:
        import cdf.core.feature_flags as ff

        if alias:
            self.name = alias

        for r_name, _ in self.resources.items():
            component_id = f"{self.base_component_id}:{r_name}"
            self.flags.update(ff.get_flags_for_component(component_id))

    @property
    def base_component_id(self) -> str:
        return f"source:{self.name}"

    def component_id(self, resource_name: str) -> str:
        return f"{self.base_component_id}:{resource_name}"

    def resource_flag_enabled(self, resource_name: str) -> bool:
        return self.flags.get(self.component_id(resource_name), False)


def to_cdf_meta(*funcs: t.Callable) -> t.Dict[str, t.Callable]:
    return {f.__name__: f for f in funcs}


source = partial(dlt.source, _impl_cls=CDFSource)  # type: ignore
"""A wrapper around dlt.source that registers the source class with the registry."""

resource = dlt.resource  # type: ignore
"""A wrapper around dlt.resource."""

__all__ = ["CDFSource", "source", "resource", "to_cdf_meta"]
