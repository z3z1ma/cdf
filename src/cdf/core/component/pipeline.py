"""The source class for continuous data framework sources."""
import fnmatch
import functools
import inspect
import os
import tempfile
import typing as t
from dataclasses import dataclass, field

import dlt
from dlt.common.destination.capabilities import TLoaderFileFormat
from dlt.common.pipeline import LoadInfo
from dlt.common.typing import TDataItem
from dlt.pipeline.pipeline import Pipeline
from dlt.sources import DltResource as CDFResource
from dlt.sources import DltSource as CDFSource

import cdf.core.feature_flags as ff

if t.TYPE_CHECKING:
    from cdf.core.workspace import Workspace


P = t.ParamSpec("P")


Metric = t.Union[float, int]
MetricAccumulator = t.Callable[[TDataItem, Metric], Metric]
MetricDefs = t.Dict[str, MetricAccumulator]


CDFPipeline = t.Generator[CDFSource, Pipeline, LoadInfo]
CDFPipelineSpec = t.Callable[..., CDFPipeline | CDFSource] | CDFPipeline | CDFSource


def _basic_pipe(source: CDFSource) -> CDFPipeline:
    """Wraps a source to conform to our expected interface.

    Args:
        source (CDFSource): The source to run.

    Yields:
        CDFSource: The source to run.

    Sends:
        SupportsPipelineClients: The pipeline object.

    Returns:
        LoadInfo: The load info.
    """
    pipeline = yield source
    return pipeline.run(source)


@dataclass
class pipeline_spec:
    """A pipeline specification."""

    pipe: CDFPipelineSpec
    """The pipeline coroutine or cdf source."""
    name: str | None = None
    """The name of the pipeline. Inferred from the pipe function if not provided."""
    version: int = 1
    """The pipeline version. This is appended to the target dataset name."""
    owners: t.Sequence[str] = ()
    """The owners of this pipeline."""
    description: str = ""
    """A description of this pipeline."""
    tags: t.Sequence[str] = ()
    """Tags for this pipeline used for component queries."""
    cron: str | None = None
    """A cron expression for scheduling this pipeline."""
    metrics: t.Dict[str, MetricDefs] = field(default_factory=dict)
    """A dict of resource names to metric definitions.

    Metrics are captured on a per resource basis during pipeline execution and are
    accumulated into this dict. The metric definitions are callables that take
    the current item and the current metric value and return the new metric value.
    """
    loader_file_format: TLoaderFileFormat | None = None
    """Set the format to be used when loading data, IE parquet, jsonl

    If max_table_nesting or a complex type is detected in the source, we will automatically
    coerce the loader format to jsonl by default.
    """
    enabled: bool = True
    """Whether this pipeline is enabled."""

    def unwrap(self, **kwargs) -> CDFPipeline:
        """Unwrap the pipeline spec into a PipeGen.

        Args:
            **kwargs: The kwargs to pass to the pipeline function.

        Returns:
            PipeGen: The unwrapped pipeline. The first next call will return a source after which the
            generator will suspend itself and await a pipeline object to be sent. The callee can
            freely manipulate the source in place before sending or can simply keep the source and
            close the generator. Business logic can be codified by the end user based on the interface.
        """
        if self.name is None:
            raise ValueError("Pipeline name must be provided.")
        dlt.pipeline(self.name)  # Generate namespace
        ctx = self.pipe(**kwargs) if callable(self.pipe) else self.pipe
        if isinstance(ctx, CDFSource):
            ctx = _basic_pipe(ctx)
        return ctx

    def __post_init__(self) -> None:
        if self.name is None:
            self.name = (
                self.pipe.name
                if isinstance(self.pipe, CDFSource)
                else self.pipe.__name__
            )

        self.description = inspect.cleandoc(self.description)

        _pipe = self.pipe
        _metrics = {}

        def _run(
            workspace: "Workspace",
            sink: str,
            resources: t.List[str] | None = None,
            **kwargs,
        ) -> LoadInfo:
            """Run the pipeline."""
            nonlocal _metrics, _pipe

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

            def agg_map(resource: str, metric_name: str, fn: MetricAccumulator):
                def _aggregator(item):
                    _metrics[resource][metric_name] = fn(
                        item, _metrics[resource][metric_name]
                    )
                    return item

                return _aggregator

            # Integrate metric capture
            for resource, metric_defs in self.metrics.items():
                _metrics.setdefault(resource, {})
                for metric_name, fn in metric_defs.items():
                    _metrics[resource].setdefault(metric_name, 0)
                    source.resources[resource].add_map(
                        agg_map(resource, metric_name, fn)
                    )

            destination, staging, _ = workspace.sinks[sink].unwrap()
            tmpdir = tempfile.TemporaryDirectory()
            try:
                p = dlt.pipeline(
                    self.name,  # type: ignore
                    dataset_name=f"{self.name}_v{self.version}",
                    progress=os.getenv("CDF_PROGRESS", "alive_progress"),  # type: ignore
                    pipelines_dir=tmpdir.name,
                    destination=destination,
                    staging=staging,
                )
                has_complex_type = any(
                    typ.get("data_type") == "complex"
                    for table in source.schema.tables.values()
                    for typ in table.get("columns", {}).values()
                )
                if self.loader_file_format:
                    p.run = functools.partial(
                        p.run, loader_file_format=self.loader_file_format
                    )
                elif (
                    (source.max_table_nesting is not None or has_complex_type)
                    and destination
                    and destination.capabilities().preferred_loader_file_format
                    == "parquet"  # Ensure parquet is coerced to jsonl if we have complex types
                ):
                    p.run = functools.partial(p.run, loader_file_format="jsonl")
                ctx.send(p)
                raise RuntimeError("Pipeline did not complete.")
            except StopIteration as e:
                load_info = t.cast(LoadInfo, e.value)
                return load_info
            except Exception as e:
                raise e
            finally:
                tmpdir.cleanup()

        _run.__wrapped__ = _pipe
        self.run = _run
        self.runtime_metrics = _metrics

    def __call__(
        self,
        workspace: "Workspace",
        sink: str,
        resources: t.List[str] | None = None,
        **kwargs,
    ) -> LoadInfo:
        """Run the pipeline.

        Args:
            workspace (Workspace): The workspace.
            sink (str): The sink to run the pipeline on.
            resources (t.List[str] | None, optional): The resources to run. Defaults to None.

        Returns:
            LoadInfo: The load info.
        """
        return self.run(workspace, sink, resources, **kwargs)


__all__ = ["CDFSource", "CDFResource", "pipeline_spec"]
