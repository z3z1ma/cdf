"""The spec classes for continuous data framework pipelines."""

import atexit
import decimal
import fnmatch
import time
import types
import typing as t

import dlt
from dlt.common.typing import TDataItem

import cdf.core.logger as logger
from cdf.core.specification.base import (
    ComponentSpecification,
    Packageable,
    PythonEntrypoint,
    PythonScript,
    Schedulable,
)

Metric = t.Union[float, int, decimal.Decimal]
MetricState = t.Dict[str, t.Dict[str, Metric]]


class MetricInterface(t.Protocol):
    def __call__(
        self, item: TDataItem, metric: t.Optional[t.Any] = None, /
    ) -> Metric: ...


class PipelineMetricSpecification(ComponentSpecification, PythonEntrypoint):
    """Defines metrics which can be captured during pipeline execution"""

    options: t.Dict[str, t.Any] = {}
    """
    Kwargs to pass to the metric function.

    This assumes the metric is a callable which accepts kwargs and returns a metric
    interface. If the metric is already a metric interface, this should be left empty.
    """

    @property
    def func(self) -> MetricInterface:
        if self.options:
            return self.main(**self.options)
        return self.main

    def __call__(self, resource: dlt.sources.DltResource, state: MetricState) -> None:
        """Adds a metric aggregator to a resource"""
        func = self.func
        first = True
        resource_name = resource.name
        metric_name = self.name
        elapsed = 0.0

        def _aggregator(item):
            nonlocal first, elapsed
            compstart = time.perf_counter()
            if first:
                state[resource_name][metric_name] = func(item)
                first = False
                return item
            state[resource_name][metric_name] = func(
                item,
                state[resource_name][metric_name],
            )
            compend = time.perf_counter()
            elapsed += compend - compstart
            return item

        state.setdefault(resource_name, {})
        resource.add_map(_aggregator)

        def _timing_stats():
            logger.info(
                f"Collecting metric {metric_name} for {resource_name} took {elapsed} seconds"
            )

        atexit.register(_timing_stats)


InlineMetricSpecifications = t.Dict[str, t.List[PipelineMetricSpecification]]
"""Mapping of resource name glob patterns to metric specs"""


class FilterInterface(t.Protocol):
    def __call__(self, item: TDataItem) -> bool: ...


class PipelineFilterSpecification(ComponentSpecification, PythonEntrypoint):
    """Defines filters which can be applied to pipeline execution"""

    options: t.Dict[str, t.Any] = {}
    """
    Kwargs to pass to the filter function. 

    This assumes the filter is a callable which accepts kwargs and returns a filter
    interface. If the filter is already a filter interface, this should be left empty.
    """

    @property
    def func(self) -> FilterInterface:
        if self.options:
            return self.main(**self.options)
        return self.main

    def __call__(self, resource: dlt.sources.DltResource) -> None:
        """Adds a filter to a resource"""
        resource.add_filter(self.func)


InlineFilterSpecifications = t.Dict[str, t.List[PipelineFilterSpecification]]
"""Mapping of resource name glob patterns to filter specs"""


class PipelineSpecification(
    ComponentSpecification, PythonScript, Packageable, Schedulable
):
    """A pipeline specification."""

    metrics: InlineMetricSpecifications = {}
    """
    A dict of resource name glob patterns to metric definitions.

    Metrics are captured on a per resource basis during pipeline execution and are
    accumulated into the metric_state dict. The metric definitions are callables that
    take the current item and the current metric value and return the new metric value.
    """
    filters: InlineFilterSpecifications = {}
    """
    A dict of resource name glob patterns to filter definitions.

    Filters are applied on a per resource basis during pipeline execution. The filter
    definitions are callables that take the current item and return a boolean indicating
    whether the item should be filtered out.
    """

    _metric_state: t.Dict[str, t.Dict[str, Metric]] = {}
    """Container for runtime metrics."""

    @property
    def metric_state(self) -> types.MappingProxyType[str, t.Dict[str, Metric]]:
        """Get a read only view of the runtime metrics."""
        return types.MappingProxyType(self._metric_state)

    def __call__(self, source: dlt.sources.DltSource) -> dlt.sources.DltSource:
        """Apply metrics and filters to a source."""
        for resource in source.selected_resources.values():
            for patt, metric in self.metrics.items():
                if fnmatch.fnmatch(resource.name, patt):
                    for applicator in metric:
                        applicator(resource, self._metric_state)
            for patt, filter_ in self.filters.items():
                if fnmatch.fnmatch(resource.name, patt):
                    for applicator in filter_:
                        applicator(resource)
        return source


def create_pipeline(name: str, data: t.Any) -> PipelineSpecification:
    data.setdefault("name", name)
    if "script_path" not in data:
        data["script_path"] = f"{name}_pipeline.py"
    return PipelineSpecification.model_validate(data)


__all__ = ["PipelineSpecification", "create_pipeline"]
