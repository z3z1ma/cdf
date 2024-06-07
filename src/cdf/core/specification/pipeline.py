"""The spec classes for continuous data framework pipelines."""

import atexit
import decimal
import fnmatch
import time
import typing as t

import dlt
import pydantic
from dlt.common.destination.exceptions import DestinationLoadingViaStagingNotSupported
from dlt.common.typing import TDataItem

import cdf.core.logger as logger
from cdf.core.specification.base import PythonEntrypoint, PythonScript, Schedulable

T = t.TypeVar("T")
TPipeline = t.TypeVar("TPipeline", bound=dlt.Pipeline)

Metric = t.Union[float, int, decimal.Decimal]
MetricStateContainer = t.MutableMapping[str, t.MutableMapping[str, Metric]]


class MetricInterface(t.Protocol):
    def __call__(
        self, item: TDataItem, metric: t.Optional[t.Any] = None, /
    ) -> Metric: ...


class PipelineMetricSpecification(PythonEntrypoint):
    """Defines metrics which can be captured during pipeline execution"""

    options: t.Dict[str, t.Any] = {}
    """Kwargs to pass to the metric function.

    This assumes the metric is a callable which accepts kwargs and returns a metric
    interface. If the metric is not parameterized, this should be left empty.
    """

    @property
    def func(self) -> MetricInterface:
        """A typed property to return the metric function"""
        if self.options:
            return self.main(**self.options)
        return self.main

    def __call__(
        self, resource: dlt.sources.DltResource, state: MetricStateContainer
    ) -> None:
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
            logger.debug(
                f"Collecting metric {metric_name} for {resource_name} took {elapsed} seconds"
            )

        atexit.register(_timing_stats)


InlineMetricSpecifications = t.Dict[str, t.List[PipelineMetricSpecification]]
"""Mapping of resource name glob patterns to metric specs"""


class FilterInterface(t.Protocol):
    def __call__(self, item: TDataItem) -> bool: ...


class PipelineFilterSpecification(PythonEntrypoint):
    """Defines filters which can be applied to pipeline execution"""

    options: t.Dict[str, t.Any] = {}
    """Kwargs to pass to the filter function. 

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


class PipelineSpecification(PythonScript, Schedulable):
    """A pipeline specification."""

    metrics: InlineMetricSpecifications = {}
    """A dict of resource name glob patterns to metric definitions.

    Metrics are captured on a per resource basis during pipeline execution and are
    accumulated into the metric_state dict. The metric definitions are callables that
    take the current item and the current metric value and return the new metric value.
    """
    filters: InlineFilterSpecifications = {}
    """A dict of resource name glob patterns to filter definitions.

    Filters are applied on a per resource basis during pipeline execution. The filter
    definitions are callables that take the current item and return a boolean indicating
    whether the item should be filtered out.
    """
    dataset_name: str = "{name}_v{version}"
    """The name of the dataset associated with the pipeline.

    Defaults to the versioned name. This string is formatted with the pipeline name, version, meta, and tags.
    """
    options: t.Dict[str, t.Any] = {}
    """Options available in pipeline scoped dlt config resolution."""
    persist_extract_package: bool = True
    """Whether to persist the extract package in the project filesystem."""

    _folder = "pipelines"
    """The folder where pipeline scripts are stored."""

    @pydantic.model_validator(mode="after")
    def _validate_dataset(self: "PipelineSpecification") -> "PipelineSpecification":
        """Validate the dataset name and apply formatting."""
        name = self.dataset_name.format(
            name=self.name, version=self.version, meta=self.meta, tags=self.tags
        ).strip()
        self.dataset_name = name or self.versioned_name
        return self

    def inject_metrics_and_filters(
        self, source: dlt.sources.DltSource, container: MetricStateContainer
    ) -> dlt.sources.DltSource:
        """Apply metrics and filters defined by the specification to a source.

        For a source to conform to the specification, it must have this method applied to it. You
        can manipulate sources without this method, but the metrics and filters will not be applied.

        Args:
            source: The source to apply metrics and filters to.
            container: The container to store metric state in. This is mutated during execution.

        Returns:
            dlt.sources.DltSource: The source with metrics and filters applied.
        """
        for resource in source.selected_resources.values():
            for patt, metric in self.metrics.items():
                if fnmatch.fnmatch(resource.name, patt):
                    for applicator in metric:
                        applicator(resource, container)
            for patt, filter_ in self.filters.items():
                if fnmatch.fnmatch(resource.name, patt):
                    for applicator in filter_:
                        applicator(resource)
        return source

    def create_pipeline(
        self,
        klass: t.Type[TPipeline] = dlt.Pipeline,
        /,
        **kwargs: t.Any,
    ) -> TPipeline:
        """Convert the pipeline specification to a dlt pipeline object.

        This is a convenience method to create a dlt pipeline object from the specification. The
        dlt pipeline is expected to use the name and dataset name from the specification. This
        is what allows declarative definitions to be associated with runtime artifacts.

        Args:
            klass (t.Type[TPipeline], optional): The pipeline class to use. Defaults to dlt.Pipeline.
            **kwargs: Additional keyword arguments to pass to the dlt.pipeline constructor.

        Returns:
            TPipeline: The dlt pipeline object.
        """
        try:
            pipe = dlt.pipeline(
                pipeline_name=self.name,
                dataset_name=self.dataset_name,
                **kwargs,
                _impl_cls=klass,
            )
        except DestinationLoadingViaStagingNotSupported:
            logger.warning(
                "Destination does not support loading via staging. Disabling staging."
            )
            kwargs.pop("staging", None)
            pipe = dlt.pipeline(
                pipeline_name=self.name,
                dataset_name=self.dataset_name,
                **kwargs,
                _impl_cls=klass,
            )
        setattr(pipe, "specification", self)
        return pipe


__all__ = ["PipelineSpecification"]
