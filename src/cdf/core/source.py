"""The source class for continuous data framework sources."""
import fnmatch
import os
import tempfile
import typing as t
from dataclasses import dataclass, field

import dlt
from dlt.common.destination.reference import JobClientBase, TDestinationReferenceArg
from dlt.common.pipeline import LoadInfo, SupportsPipeline
from dlt.common.typing import TDataItem
from dlt.destinations.sql_client import SqlClientBase
from dlt.sources import DltResource as CDFResource
from dlt.sources import DltSource as CDFSource

import cdf.core.constants as c
from cdf.core.feature_flags import apply_feature_flags, get_or_create_flag_dispatch

if t.TYPE_CHECKING:
    from cdf.core.workspace import Workspace

P = t.ParamSpec("P")

Metric = t.Union[float, int]
MetricAccumulator = t.Callable[[TDataItem, Metric], Metric]
MetricDefs = t.Dict[str, MetricAccumulator]


class SupportsPipelineClients(SupportsPipeline, t.Protocol):
    def sql_client(
        self, schema_name: str | None = None, credentials: t.Any = None
    ) -> SqlClientBase[t.Any]:
        ...

    def destination_client(
        self, schema_name: str | None = None, credentials: t.Any = None
    ) -> JobClientBase:
        ...


PipeGen = t.Generator[CDFSource, SupportsPipelineClients, LoadInfo]
CDFPipeline = t.Callable[..., PipeGen | CDFSource] | PipeGen


def _basic_pipe(source: CDFSource) -> PipeGen:
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


class SinkOptions(t.TypedDict, total=False):
    """The sink options."""

    destination: TDestinationReferenceArg
    credentials: t.Any
    staging: TDestinationReferenceArg


@dataclass
class pipeline_spec:
    pipe: CDFPipeline
    """The pipeline generator function or cdf source."""
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
    enabled: bool = True
    """Whether this pipeline is enabled."""

    def unwrap(self, **kwargs) -> PipeGen:
        """Unwrap the pipeline spec into a PipeGen.

        Args:
            **kwargs: The kwargs to pass to the pipeline function.

        Returns:
            PipeGen: The unwrapped pipeline. The first next call will return a source after which the
            generator will suspend itself and await a pipeline object to be sent. The callee can
            freely manipulate the source in place before sending or can simply keep the source and
            close the generator. Business logic can be codified by the end user based on the interface.
        """
        ctx = self.pipe(**kwargs) if callable(self.pipe) else self.pipe
        if isinstance(ctx, CDFSource):
            ctx = _basic_pipe(ctx)
        return ctx

    def __post_init__(self) -> None:
        if self.name is None:
            self.name = self.pipe.__name__

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

            feature_flags, meta = get_or_create_flag_dispatch(
                None,
                source=source,
                workspace=workspace,
            )

            if config_hash := meta.get("config_hash"):
                workspace.raise_on_ff_lock_mismatch(config_hash)

            if resources:
                # Prioritize explicit resource selection
                for name, resource in source.resources.items():
                    resource.selected = any(
                        fnmatch.fnmatch(name, pattern) for pattern in resources
                    )
            else:
                # Use feature flags to select resources if no explicit selection
                apply_feature_flags(
                    source,
                    feature_flags,
                    workspace=workspace,
                    raise_on_no_resources=True,
                )

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

            # Get sink config
            # TODO: migrate to dlt 0.4.
            sink_opts = t.cast(SinkOptions, workspace.sinks[sink].ingest)
            if "BUCKET_URL" in os.environ:
                sink_opts["staging"] = "filesystem"

            tmpdir = tempfile.TemporaryDirectory()
            try:
                ctx.send(
                    dlt.pipeline(
                        f"cdf-{self.name}",
                        dataset_name=f"{self.name}_v{self.version}",
                        progress=os.getenv("CDF_PROGRESS", "alive_progress"),  # type: ignore
                        pipelines_dir=tmpdir.name,
                        **sink_opts,
                    )
                )
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


def export_pipelines(*pipelines: pipeline_spec, scope: dict | None = None) -> None:
    """Export pipelines to the callers global scope.

    Args:
        *pipelines (pipeline_spec): The pipelines to export.
        scope (dict | None, optional): The scope to export to. Defaults to globals().
    """
    if scope is None:
        import inspect

        frame = inspect.currentframe()
        if frame is not None:
            frame = frame.f_back
        if frame is not None:
            scope = frame.f_globals

    (scope or globals()).setdefault(c.CDF_PIPELINES, []).extend(pipelines)


__all__ = ["CDFSource", "CDFResource", "pipeline_spec", "export_pipelines"]
