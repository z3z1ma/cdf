"""The source class for continuous data flow sources."""
import typing as t
from dataclasses import dataclass, field
from functools import partial

import dlt
from dlt.common.typing import TDataItem
from dlt.sources import DltResource as CDFResource
from dlt.sources import DltSource as CDFSource

LazySource = t.Callable[..., CDFSource]

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


# Reserving the ability to augment the source wrapper in the future
source = partial(dlt.source, _impl_cls=CDFSource)
resource = dlt.resource

__all__ = [
    "CDFSource",
    "CDFResource",
    "CDFSourceWrapper",
    "LazySource",
    "source",
    "resource",
]
