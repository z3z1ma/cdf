"""The spec classes for continuous data framework pipelines."""

import atexit
import decimal
import fnmatch
import time
import types
import typing as t

import dlt
import pydantic
from dlt.common.typing import TDataItem

import cdf.core.logger as logger
from cdf.core.specification.base import PythonEntrypoint, PythonScript, Schedulable

T = t.TypeVar("T")

Metric = t.Union[float, int, decimal.Decimal]
MetricState = t.Dict[str, t.Dict[str, Metric]]


class MetricInterface(t.Protocol):
    def __call__(
        self, item: TDataItem, metric: t.Optional[t.Any] = None, /
    ) -> Metric: ...


class PipelineMetricSpecification(PythonEntrypoint):
    """Defines metrics which can be captured during pipeline execution"""

    options: t.Dict[str, t.Any] = pydantic.Field(
        default_factory=dict,
        description="Kwargs to pass to the metric function if it is a callable that returns a metric interface. If the metric is already a metric interface, this should be left empty.",
    )
    """
    Kwargs to pass to the metric function.

    This assumes the metric is a callable which accepts kwargs and returns a metric
    interface. If the metric is already a metric interface, this should be left empty.
    """

    @property
    def func(self) -> MetricInterface:
        """A typed property to return the metric function"""
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

        def _aggregator(item: T) -> T:
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


class PipelineFilterSpecification(PythonEntrypoint):
    """Defines filters which can be applied to pipeline execution"""

    options: t.Dict[str, t.Any] = pydantic.Field(
        default_factory=dict,
        description="Kwargs to pass to the filter function if it is a callable that returns a filter interface. If the filter is already a filter interface, this should be left empty.",
    )
    """
    Kwargs to pass to the filter function. 

    This assumes the filter is a callable which accepts kwargs and returns a filter
    interface. If the filter is already a filter interface, this should be left empty.
    """

    @property
    def func(self) -> FilterInterface:
        """A typed property to return the filter function"""
        if self.options:
            return self.main(**self.options)
        return self.main

    def __call__(self, resource: dlt.sources.DltResource) -> None:
        """Adds a filter to a resource"""
        resource.add_filter(self.func)


InlineFilterSpecifications = t.Dict[str, t.List[PipelineFilterSpecification]]
"""Mapping of resource name glob patterns to filter specs"""


class SchemaOptions(pydantic.BaseModel):
    preferred_types: t.Optional[t.Dict[str, t.List[str]]] = None
    detections: t.Optional[t.List[str]] = None


class PipelineSpecification(PythonScript, Schedulable):
    """A pipeline specification."""

    metrics: InlineMetricSpecifications = pydantic.Field(
        default_factory=dict,
        description="A dict of resource name glob patterns to metric definitions.",
    )
    """
    A dict of resource name glob patterns to metric definitions.

    Metrics are captured on a per resource basis during pipeline execution and are
    accumulated into the metric_state dict. The metric definitions are callables that
    take the current item and the current metric value and return the new metric value.
    """
    filters: InlineFilterSpecifications = pydantic.Field(
        default_factory=dict,
        description="A dict of resource name glob patterns to filter definitions.",
    )
    """
    A dict of resource name glob patterns to filter definitions.

    Filters are applied on a per resource basis during pipeline execution. The filter
    definitions are callables that take the current item and return a boolean indicating
    whether the item should be filtered out.
    """

    dataset_name: str = pydantic.Field(
        "{name}_v{version}",
        description="The name of the dataset associated with the pipeline. Defaults to the versioned name. This string is formatted with the pipeline name, version, meta, and tags.",
    )
    """The name of the dataset associated with the pipeline."""

    _metric_state: t.Dict[str, t.Dict[str, Metric]] = {}
    """Container for runtime metrics."""

    _folder = "pipelines"
    """The folder where pipeline scripts are stored."""

    @pydantic.model_validator(mode="after")
    def _setup_pipeline(self: "PipelineSpecification") -> "PipelineSpecification":
        self.dataset_name = self.dataset_name.format(
            name=self.name,
            version=self.version,
            meta=self.meta,
            tags=self.tags,
        ).strip()
        if not self.dataset_name:
            self.dataset_name = self.versioned_name
        return self

    @property
    def runtime_metrics(self) -> types.MappingProxyType[str, t.Dict[str, Metric]]:
        """Get a read only view of the runtime metrics."""
        return types.MappingProxyType(self._metric_state)

    def apply(self, source: dlt.sources.DltSource) -> dlt.sources.DltSource:
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


__all__ = ["PipelineSpecification"]
