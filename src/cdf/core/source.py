"""The source class for continuous data flow sources."""
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
    pipeline_name: str
    """The name of the pipeline."""
    pipeline_gen: CDFPipeline
    """The pipeline generator function."""
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
    enabled = True
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
        ctx = (
            self.pipeline_gen(**kwargs)
            if callable(self.pipeline_gen)
            else self.pipeline_gen
        )
        if isinstance(ctx, CDFSource):
            ctx = _basic_pipe(ctx)
        return ctx

    def __post_init__(self) -> None:
        _pipe = self.pipeline_gen
        _metrics = {}

        def _run(
            resources: t.List[str] | None = None,
            sink_opts: SinkOptions | None = None,
            apply_flags=lambda src: src,
            **kwargs,
        ) -> LoadInfo:
            nonlocal _metrics, _pipe

            ctx = self.unwrap(**kwargs)
            source = next(ctx)

            if resources is None:
                # Use feature flags to select resources
                apply_flags(source)
            else:
                # Prioritize explicit resource selection
                for name, resource in source.resources.items():
                    resource.selected = name in resources

            for resource, metric_defs in self.metrics.items():
                _metrics.setdefault(resource, {})
                for metric_name, fn in metric_defs.items():
                    _metrics[resource].setdefault(metric_name, 0)

                    def agg(item) -> Metric:
                        _metrics[resource][metric_name] = fn(
                            item, _metrics[resource][metric_name]
                        )
                        return item

                    source.resources[resource].add_map(agg)

            tmpdir = tempfile.TemporaryDirectory()
            try:
                ctx.send(
                    dlt.pipeline(
                        f"cdf-{self.pipeline_name}",
                        dataset_name=f"{self.pipeline_name}_v{self.version}",
                        progress=os.getenv("CDF_PROGRESS", "alive_progress"),  # type: ignore
                        pipelines_dir=tmpdir.name,
                        **(sink_opts or {}),
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
        resources: t.List[str],
        sink_opts: SinkOptions,
        apply_flags=lambda src: src,
        **kwargs,
    ) -> LoadInfo:
        return self.run(resources, sink_opts, apply_flags, **kwargs)


def export_pipelines(*pipelines: pipeline_spec, scope: dict | None = None) -> None:
    """Export sources to the callers global scope.

    Args:
        pipelines (pipeline_spec): The pipelines to export.
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
