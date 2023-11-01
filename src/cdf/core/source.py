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
        # FF Stuff here
        # TODO: feature flags needs an abstract provider, the basic implementation should use 2
        # local files. One in the user home directory and one local to the repository
        # This is an opinionated design, and we will enforce specific naming and locations
        # 2nd implementation should use a FF service such as harness.io or launchdarkly --
        # we will use harness.io because, well, y'know
        register_source(source=self)


def to_cdf_meta(*funcs: t.Callable) -> t.Dict[str, t.Callable]:
    return {f.__name__: f for f in funcs}


source = partial(dlt.source, _impl_cls=ContinuousDataFlowSource)  # type: ignore
"""A wrapper around dlt.source that registers the source class with the registry."""

resource = dlt.resource  # type: ignore
"""A wrapper around dlt.resource."""

__all__ = ["ContinuousDataFlowSource", "source", "resource", "to_cdf_meta"]
