"""The source class for continuous data flow sources."""
import typing as t
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

Metric = t.Union[float, int]
MetricAccumulator = t.Callable[[TDataItem, Metric], Metric]
MetricDefs = t.Dict[str, MetricAccumulator]


@dataclass
class CDFSourceWrapper:
    factory: LazySource
    version: int = 1
    owners: t.Sequence[str] = ()
    description: str = ""
    tags: t.Sequence[str] = ()
    cron: str | None = None
    metrics: t.Dict[str, MetricDefs] = field(default_factory=dict)
    enabled = True

    def __post_init__(self) -> None:
        source = None
        metrics = {}
        base_factory = self.factory

        def _factory(*args, **kwargs) -> CDFSource:
            nonlocal metrics, source

            if source is None:
                # Create source
                source = base_factory(*args, **kwargs)

                # Add flags
                ...

                # Add metrics
                for resource, metric_defs in self.metrics.items():
                    metrics.setdefault(resource, {})
                    for metric_name, fn in metric_defs.items():
                        metrics[resource].setdefault(metric_name, 0)

                        def agg(item) -> Metric:
                            metrics[resource][metric_name] = fn(
                                item, metrics[resource][metric_name]
                            )
                            return item

                        source.resources[resource].add_map(agg)

            # Return prepared source
            return source

        _factory.__wrapped__ = base_factory
        self.factory = _factory
        self.runtime_metrics = metrics

    def __call__(self, *args, **kwargs) -> CDFSource:
        return self.factory(*args, **kwargs)


source = partial(dlt.source, _impl_cls=CDFSource)
"""A wrapper around dlt.source that registers the source class with the registry."""

resource = dlt.resource  # type: ignore
"""A wrapper around dlt.resource. Reserving this for future use."""

__all__ = [
    "CDFSource",
    "CDFSourceWrapper",
    "LazySource",
    "source",
    "resource",
]
