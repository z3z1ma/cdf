"""The source class for continuous data flow sources."""
import typing as t
from dataclasses import dataclass, field

from dlt.common.typing import TDataItem
from dlt.sources import DltResource as CDFResource
from dlt.sources import DltSource as CDFSource

import cdf.core.constants as c

LazySource = t.Callable[..., CDFSource]

Metric = t.Union[float, int]
MetricAccumulator = t.Callable[[TDataItem, Metric], Metric]
MetricDefs = t.Dict[str, MetricAccumulator]


@dataclass
class source_spec:
    factory: LazySource
    """A function that returns a dlt source."""
    version: int = 1
    """The source version. This is appended to the target dataset name."""
    owners: t.Sequence[str] = ()
    """The owners of this source."""
    description: str = ""
    """A description of this source."""
    tags: t.Sequence[str] = ()
    """Tags for this source used for component queries."""
    cron: str | None = None
    """A cron expression for scheduling this source."""
    metrics: t.Dict[str, MetricDefs] = field(default_factory=dict)
    """A dict of resource names to metric definitions.

    Metrics are captured on a per resource basis during source execution and are
    accumulated into this dict. The metric definitions are callables that take
    the current item and the current metric value and return the new metric value.
    """
    enabled = True
    """Whether this source is enabled."""

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


def export_sources(*, scope: dict | None = None, **sources: source_spec) -> None:
    """Export sources to the callers global scope.

    Args:
        scope (dict | None, optional): The scope to export to. Defaults to globals().
        **sources (source_spec): The sources to export.
    """
    if scope is None:
        import inspect

        frame = inspect.currentframe()
        if frame is not None:
            frame = frame.f_back
        if frame is not None:
            scope = frame.f_globals

    (scope or globals()).setdefault(c.CDF_SOURCE, {}).update(sources)


__all__ = ["CDFSource", "CDFResource", "source_spec", "export_sources"]
