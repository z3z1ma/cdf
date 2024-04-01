"""The spec classes for continuous data framework pipelines."""

import atexit
import decimal
import fnmatch
import os
import time
import types
import typing as t
from pathlib import Path

import dlt
import dynaconf
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

    options: t.Dict[str, t.Any] = {}
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

    options: t.Dict[str, t.Any] = {}
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


class PipelineSpecification(PythonScript, Schedulable):
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

    dataset_name: str = ""
    """The name of the dataset associated with the pipeline."""

    _metric_state: t.Dict[str, t.Dict[str, Metric]] = {}
    """Container for runtime metrics."""

    _folder = "pipelines"
    """The folder where pipeline scripts are stored."""

    @pydantic.model_validator(mode="after")
    def _setup(self: "PipelineSpecification") -> "PipelineSpecification":
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
    def metric_state(self) -> types.MappingProxyType[str, t.Dict[str, Metric]]:
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

    # TODO: type _wrapper with a protocol
    @property
    def main(self) -> t.Callable[..., t.Any]:
        main = super().main

        # These should stay in regular entrypoint
        # and are the only definable kwargs?
        # import_schema_path: str = None,
        # export_schema_path: str = None,
        # pipelines_dir: str = None,

        # NOTE: we _could_ keep dataset_name as a kwarg with the caveat that it breaks assumptions if we do not load our primary
        # payload to the statically configured dataset at least once in the pipeline

        # New entrypoint below
        # our primary inputs are the parameterized destination
        def _wrapper(
            destination: str,  # this will come from the CLI, hydrated from ./sinks or ./destinations
            staging: t.Optional[
                str
            ] = None,  # this is included in the above? though I could see selectively staging per pipeline
            progress: t.Optional[str] = None,
        ) -> t.Any:
            pipeline_name = self.name
            dataset_name = self.dataset_name

            # 5 vars to inject into context before invoking main
            # pipeline_name, dataset_name, destination, staging, progress

            return main()

        return _wrapper

    @classmethod
    def from_config(
        cls, name: str, root: Path, config: dynaconf.Dynaconf
    ) -> "PipelineSpecification":
        config.setdefault("name", name)
        config.setdefault("path", f"{name}_pipeline.py")
        config["workspace_path"] = root
        return cls.model_validate(config, from_attributes=True)


__all__ = ["PipelineSpecification"]
