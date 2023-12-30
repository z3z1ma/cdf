"""The spec classes for continuous data framework pipelines."""
import atexit
import decimal
import fnmatch
import functools
import logging
import tempfile
import time
import types
import typing as t

import dlt
import pydantic
from dlt.common.destination.capabilities import TLoaderFileFormat
from dlt.common.pipeline import LoadInfo
from dlt.common.runtime.collector import LogCollector
from dlt.common.schema.typing import TSchemaEvolutionMode
from dlt.common.typing import TDataItem
from dlt.pipeline.pipeline import Pipeline
from dlt.sources import DltResource as CDFResource
from dlt.sources import DltSource as CDFSource
from typing_extensions import TypedDict

import cdf.core.constants as c
import cdf.core.context as context
import cdf.core.feature_flags as ff
import cdf.core.logger as logger
from cdf.core.spec.base import ComponentSpecification, Packageable, Schedulable

if t.TYPE_CHECKING:
    from cdf.core.workspace import Workspace


class TSchemaContractDict(TypedDict, total=False):
    tables: TSchemaEvolutionMode | None
    columns: TSchemaEvolutionMode | None
    data_type: TSchemaEvolutionMode | None


TSchemaContract = TSchemaEvolutionMode | TSchemaContractDict


Metric = float | int | decimal.Decimal
MetricState = t.Dict[str, t.Dict[str, Metric]]


class MetricInterface(t.Protocol):
    def __call__(self, item: TDataItem, metric: t.Any | None = None, /) -> Metric:
        ...


class PipelineMetricSpecification(ComponentSpecification):
    """Defines metrics which can be captured during pipeline execution"""

    input_: t.Dict[str, t.Any] = pydantic.Field({}, alias="input")
    """
    Kwargs to pass to the metric function.

    This assumes the metric is a callable which accepts kwargs and returns a metric
    interface. If the metric is already a metric interface, this should be left empty.
    """

    _key = c.METRICS
    _autoregister = False

    @property
    def func(self) -> MetricInterface:
        if self.input_:
            return self._main(**self.input_)
        return self._main

    def __call__(self, resource: CDFResource, state: MetricState) -> None:
        """Adds a metric aggregator to a resource"""
        func = self.func
        first = True
        resource_name = resource.name
        metric_name = self.name
        elapsed = 0.0

        def _aggregator(item):
            nonlocal first, elapsed
            t1 = time.perf_counter()
            if first:
                state[resource_name][metric_name] = func(item)
                first = False
                return item
            state[resource_name][metric_name] = func(
                item,
                state[resource_name][metric_name],
            )
            t2 = time.perf_counter()
            elapsed += t2 - t1
            return item

        state.setdefault(resource_name, {})
        resource.add_map(_aggregator)

        def _timing_stats():
            import cdf

            cdf.logger.info(
                f"Collecting metric {metric_name} for {resource_name} took {elapsed} seconds"
            )

        atexit.register(_timing_stats)


InlineMetricSpecifications = t.Dict[str, t.List[PipelineMetricSpecification]]
"""Mapping of resource name glob patterns to metric specs"""


class FilterInterface(t.Protocol):
    def __call__(self, item: TDataItem) -> bool:
        ...


class PipelineFilterSpecification(ComponentSpecification):
    """Defines filters which can be applied to pipeline execution"""

    input_: t.Dict[str, t.Any] = pydantic.Field({}, alias="input")
    """
    Kwargs to pass to the filter function. 

    This assumes the filter is a callable which accepts kwargs and returns a filter
    interface. If the filter is already a filter interface, this should be left empty.
    """

    _key = c.FILTERS
    _autoregister = False

    @property
    def func(self) -> FilterInterface:
        if self.input_:
            return self._main(**self.input_)
        return self._main

    def __call__(self, resource: CDFResource) -> None:
        """Adds a filter to a resource"""
        resource.add_filter(self.func)


InlineFilterSpecifications = t.Dict[str, t.List[PipelineFilterSpecification]]
"""Mapping of resource name glob patterns to filter specs"""

CooperativePipelineInterface = pydantic.SkipValidation[
    t.Generator[CDFSource, Pipeline, LoadInfo]
]
PipelineInterface = (
    t.Callable[..., CooperativePipelineInterface | CDFSource]
    | CooperativePipelineInterface
    | CDFSource
)


def _basic_pipe(source: CDFSource) -> CooperativePipelineInterface:
    """Wraps a source to conform to our expected interface."""
    pipeline = yield source
    return pipeline.run(source)


class PipelineSpecification(ComponentSpecification, Packageable, Schedulable):
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
    loader_file_format: TLoaderFileFormat | None = None
    """
    Set the format to be used when loading data, IE parquet, jsonl

    If max_table_nesting or a complex type is detected in the source, we will automatically
    coerce the loader format to jsonl by default.
    """
    schema_contract: TSchemaContract | None = None
    """The strategy to use when updating the schema."""

    _metric_state: t.Dict[str, t.Dict[str, Metric]] = {}
    _key = c.PIPELINES

    @property
    def pipe(self) -> PipelineInterface:
        """Get the pipeline."""
        return self._main

    @property
    def metric_state(self) -> types.MappingProxyType[str, t.Dict[str, Metric]]:
        """Get a read only view of the runtime metrics."""
        return types.MappingProxyType(self._metric_state)

    @pydantic.model_validator(mode="after")
    def _register_subcomponents(self):
        """Register subcomponents"""
        workspace = context.get_active_workspace()
        if not workspace:
            return self
        for _, specs in self.metrics.items():
            for spec in specs:
                ComponentSpecification.register_subcomponent(
                    spec, self.name, workspace.name
                )
        for _, specs in self.filters.items():
            for spec in specs:
                ComponentSpecification.register_subcomponent(
                    spec, self.name, workspace.name
                )
        return self

    def unwrap(self, **kwargs: t.Any) -> CooperativePipelineInterface:
        """
        Unwrap the pipeline specification into a CooperativePipelineInterface.

        Args:
            **kwargs: The kwargs to pass to the pipeline function.

        Returns:
            CooperativePipelineInterface: The unwrapped pipeline. The first
                next call will return a source after which the generator will
                suspend itself and await a pipeline object to be sent. The callee
                can freely manipulate the source in place before sending or can
                simply keep the source and close the generator. Business logic can
                be codified by the end user based on the interface.
        """

        # HACK: Generate namespace eagerly for source name scoped config resolution
        # https://github.com/dlt-hub/dlt/issues/816
        dlt.pipeline(self.name)

        ctx = self.pipe(**kwargs) if callable(self.pipe) else self.pipe
        if isinstance(ctx, CDFSource):
            ctx = _basic_pipe(ctx)

        return ctx

    def __call__(
        self,
        workspace: "Workspace",
        sink: str,
        resources: t.List[str] | None = None,
        **kwargs: t.Any,
    ) -> LoadInfo:
        """
        Run the pipeline with FFs, metrics, and resource selection.

        Args:
            workspace (Workspace): The workspace.
            sink (str): The sink to run the pipeline on.
            resources (t.List[str] | None, optional): The resources to run. Defaults to None.
            **kwargs: Additional kwargs to pass to the pipeline.

        Returns:
            LoadInfo: The load info.
        """
        ctx = self.unwrap(**kwargs)
        source = next(ctx)

        if resources:
            # Prioritize explicit resource selection
            for name, resource in source.resources.items():
                resource.selected = any(
                    fnmatch.fnmatch(name, pattern) for pattern in resources
                )
        else:
            # Use feature flags to select resources if no explicit selection
            ff.process_source(source, ff.get_provider(workspace))

        for patt, filters in self.filters.items():
            for resource in source.resources.values():
                if not fnmatch.fnmatch(resource.name, patt):
                    continue
                for fn in filters:
                    fn(resource)

        metric_state = self._metric_state
        for patt, metrics in self.metrics.items():
            for resource in source.resources.values():
                if not fnmatch.fnmatch(resource.name, patt):
                    continue
                for fn in metrics:
                    fn(resource, metric_state)

        destination, staging, _ = workspace.sinks[sink]()
        assert destination is not None, "Destination must be provided."
        tmpdir = tempfile.TemporaryDirectory()
        try:
            p1 = time.perf_counter()
            p = dlt.pipeline(
                self.name,
                dataset_name=self.versioned_name,
                progress=LogCollector(
                    log_period=5.0,
                    logger=logger.create(self.versioned_name),
                    dump_system_stats=True,
                ),
                pipelines_dir=tmpdir.name,
                destination=destination,
                staging=staging,
            )

            # Intelligent loader file format selection
            caps = destination.capabilities()
            has_nesting_limit = source.max_table_nesting is not None
            has_complex_type = any(
                typ.get("data_type") == "complex"
                for table in source.schema.tables.values()
                for typ in table.get("columns", {}).values()
            )
            has_parquet_preference = (
                caps.preferred_loader_file_format == "parquet"
                or caps.preferred_staging_file_format == "parquet"
            )
            if self.loader_file_format:
                # Prefer user specified format
                p.run = functools.partial(
                    p.run, loader_file_format=self.loader_file_format
                )
            elif (has_nesting_limit or has_complex_type) and has_parquet_preference:
                # Ensure parquet is coerced to jsonl if we have a chance for complex types
                p.run = functools.partial(p.run, loader_file_format="jsonl")

            # Passthrough schema contract to pipeline entrypoint
            p.run = functools.partial(p.run, schema_contract=self.schema_contract)
            ctx.send(p)
            raise RuntimeError("Pipeline did not complete.")
        except StopIteration as e:
            load_info = t.cast(LoadInfo, e.value)
            if t.TYPE_CHECKING:
                p1 = 0.0
                p = dlt.pipeline()
            p2 = time.perf_counter()

            # Track the metadata associated with the load job
            logger.info("Pipeline execution took %.3f seconds", p2 - p1)
            logger.info(f"Writing load info for {self.name} {self.version} {sink}")
            p.run(
                [load_info.asdict()],
                dataset_name=c.INTERNAL_SCHEMA,
                table_name=c.LOAD_INFO_TABLE,
                write_disposition="append",
            )

            # Track runtime metrics
            if any(self.metrics.values()):
                metric_state["load_ids"] = [pkg.load_id for pkg in load_info.load_packages]  # type: ignore
                logger.info(f"Computed Metrics: {metric_state}")
                logger.info(f"Writing metrics for load {metric_state['load_ids']}")
                p.run(
                    [metric_state],
                    table_name=c.METRIC_INFO_TABLE,
                    write_disposition="append",
                )
            else:
                logger.info("No runtime metrics to write")

            if p.runtime_config.slack_incoming_hook:
                for package in load_info.load_packages:
                    for schema, upd in package.schema_update.items():
                        logger.info(f"Schema update for {schema}: {upd}")

            return load_info
        except Exception as e:
            if t.TYPE_CHECKING:
                p1 = 0.0
                p = dlt.pipeline()
            p2 = time.perf_counter()

            # If we have a pipeline object, track the exception
            logger.error("Pipeline failed after %s seconds", p2 - p1)
            logger.error(f"Writing exception for {self.name} {self.version} {sink}")
            try:
                p.run(
                    [
                        {
                            "error": str(e),
                            "pipeline": self.name,
                            "version": self.version,
                            "sink": sink,
                        }
                    ],
                    dataset_name=c.INTERNAL_SCHEMA,
                    table_name=c.EXC_INFO_TABLE,
                    write_disposition="append",
                )
            except (NameError, UnboundLocalError) as write_e:
                logger.error(
                    "Pipeline object not found. Exception not written to BQ. %s",
                    write_e,
                )
            except Exception as write_e:
                logger.error("Exception not written to BQ. %s", write_e)

            raise e
        finally:
            tmpdir.cleanup()


__all__ = [
    "CDFSource",
    "CDFResource",
    "CooperativePipelineInterface",
    "PipelineSpecification",
    "PipelineInterface",
]
