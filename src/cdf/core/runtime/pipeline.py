"""The runtime pipeline module is responsible for executing pipelines from pipeline specifications.

It performs the following functions:
- Injects the runtime context into the pipeline.
- Executes the pipeline.
- Captures metrics during extract.
- Intercepts sources during extract. (if specified, this makes the pipeline a no-op)
- Applies transformations to sources during extract.
- Stages data if a staging location is provided and enabled in the runtime context.
- Forces replace disposition if specified in the runtime context.
- Filters resources based on glob patterns.
- Logs a warning if dataset_name is provided in the runtime context. (since we want to manage it)
- Creates a cdf pipeline from a dlt pipeline.
"""

import fnmatch
import os
import typing as t
from contextlib import contextmanager, nullcontext, redirect_stdout
from contextvars import ContextVar, copy_context

import dlt
from dlt.common.destination import TDestinationReferenceArg, TLoaderFileFormat
from dlt.common.pipeline import ExtractInfo, LoadInfo
from dlt.common.schema.typing import (
    TAnySchemaColumns,
    TColumnNames,
    TSchemaContract,
    TWriteDisposition,
)
from dlt.extract.extract import Extract, data_to_sources
from dlt.pipeline.pipeline import Pipeline

import cdf.core.context as context
import cdf.core.logger as logger
from cdf.core.specification import PipelineSpecification
from cdf.types import M

T = t.TypeVar("T")


def _ident(x: T) -> T:
    """An identity function."""
    return x


class RuntimeContext(t.NamedTuple):
    """The runtime context for a pipeline."""

    pipeline_name: str
    """The pipeline name."""
    dataset_name: str
    """The dataset name."""
    destination: TDestinationReferenceArg
    """The destination."""
    staging: t.Optional[TDestinationReferenceArg] = None
    """The staging location."""
    select: t.Optional[t.List[str]] = None
    """A list of glob patterns to select resources."""
    exclude: t.Optional[t.List[str]] = None
    """A list of glob patterns to exclude resources."""
    force_replace: bool = False
    """Whether to force replace disposition."""
    intercept_sources: t.Optional[t.Set[dlt.sources.DltSource]] = None
    """Stores the intercepted sources in itself if provided."""
    enable_stage: bool = True
    """Whether to stage data if a staging location is provided."""
    applicator: t.Callable[[dlt.sources.DltSource], dlt.sources.DltSource] = _ident
    """The transformation to apply to the sources."""
    metrics: t.Optional[t.Mapping[str, t.Any]] = None
    """A container for captured metrics during extract."""


CONTEXT: ContextVar[RuntimeContext] = ContextVar("runtime_context")


@contextmanager
def runtime_context(
    pipeline_name: str,
    dataset_name: str,
    destination: TDestinationReferenceArg,
    staging: t.Optional[TDestinationReferenceArg] = None,
    select: t.Optional[t.List[str]] = None,
    exclude: t.Optional[t.List[str]] = None,
    force_replace: bool = False,
    intercept_sources: t.Optional[t.Set[dlt.sources.DltSource]] = None,
    enable_stage: bool = True,
    applicator: t.Callable[[dlt.sources.DltSource], dlt.sources.DltSource] = _ident,
    metrics: t.Optional[t.Mapping[str, t.Any]] = None,
) -> t.Iterator[None]:
    """A context manager for setting the runtime context.

    This allows the cdf library to set the context prior to running the pipeline which is
    ultimately evaluating user code.
    """
    token = CONTEXT.set(
        RuntimeContext(
            pipeline_name,
            dataset_name,
            destination,
            staging,
            select,
            exclude,
            force_replace,
            intercept_sources,
            enable_stage,
            applicator,
            metrics,
        )
    )
    try:
        yield
    finally:
        CONTEXT.reset(token)


class RuntimePipeline(Pipeline):
    """Overrides certain methods of the dlt pipeline to allow for cdf specific behavior."""

    def extract(
        self,
        data: t.Any,
        *,
        table_name: str = None,  # type: ignore[arg-type]
        parent_table_name: str = None,  # type: ignore[arg-type]
        write_disposition: TWriteDisposition = None,  # type: ignore[arg-type]
        columns: TAnySchemaColumns = None,  # type: ignore[arg-type]
        primary_key: TColumnNames = None,  # type: ignore[arg-type]
        schema: dlt.Schema = None,  # type: ignore[arg-type]
        max_parallel_items: int = None,  # type: ignore[arg-type]
        workers: int = None,  # type: ignore[arg-type]
        schema_contract: TSchemaContract = None,  # type: ignore[arg-type]
    ) -> ExtractInfo:
        with self._maybe_destination_capabilities():
            sources = data_to_sources(
                data,
                self,
                schema,
                table_name,
                parent_table_name,
                write_disposition,
                columns,
                primary_key,
                schema_contract,
            )

        runtime_context = CONTEXT.get()

        def _apply_filters(
            source: dlt.sources.DltSource, resource_patterns: t.List[str], invert: bool
        ) -> dlt.sources.DltSource:
            """Filters resources in a source based on a list of patterns."""
            return source.with_resources(
                *[
                    r
                    for r in source.selected_resources
                    if any(fnmatch.fnmatch(r, patt) for patt in resource_patterns)
                    ^ invert
                ]
            )

        if runtime_context.select:
            for i, source in enumerate(sources):
                sources[i] = _apply_filters(
                    source, runtime_context.select, invert=False
                )
        else:
            active_project = context.active_project.get()
            for i, source in enumerate(sources):
                sources[i] = active_project.feature_flag_provider.apply_source(source)

        if runtime_context.exclude:
            for i, source in enumerate(sources):
                sources[i] = _apply_filters(
                    source, runtime_context.exclude, invert=True
                )

        if runtime_context.intercept_sources is not None:
            extract_step = Extract(
                self._schema_storage,
                self._normalize_storage_config(),
                self.collector,
                original_data=data,
            )
            for source in sources:
                runtime_context.intercept_sources.add(source)
            return self._get_step_info(extract_step)

        for i, source in enumerate(sources):
            sources[i] = runtime_context.applicator(source)

        if runtime_context.force_replace:
            write_disposition = "replace"

        info = super().extract(
            sources,
            table_name=table_name,
            parent_table_name=parent_table_name,
            write_disposition=write_disposition,
            columns=columns,
            primary_key=primary_key,
            schema=schema,
            max_parallel_items=max_parallel_items,
            workers=workers,
            schema_contract=schema_contract,
        )

        if runtime_context.metrics:
            super().extract(
                dlt.resource(
                    [
                        {
                            "load_id": load_id,
                            "metrics": dict(runtime_context.metrics),
                        }
                        for load_id in info.loads_ids
                    ],
                    name="cdf_runtime_metrics",
                    write_disposition="append",
                    columns=[
                        {"name": "load_id", "data_type": "text"},
                        {"name": "metrics", "data_type": "complex"},
                    ],
                    table_name="_cdf_metrics",
                )
            )

        return info

    def run(
        self,
        data: t.Any = None,
        *,
        dataset_name: str = None,  # type: ignore[arg-type]
        table_name: str = None,  # type: ignore[arg-type]
        write_disposition: TWriteDisposition = None,  # type: ignore[arg-type]
        columns: TAnySchemaColumns = None,  # type: ignore[arg-type]
        primary_key: TColumnNames = None,  # type: ignore[arg-type]
        schema: dlt.Schema = None,  # type: ignore[arg-type]
        loader_file_format: TLoaderFileFormat = None,  # type: ignore[arg-type]
        schema_contract: TSchemaContract = None,  # type: ignore[arg-type]
    ) -> LoadInfo:
        if dataset_name is not None:
            logger.warning(
                "Using dataset_name is a cdf pipeline should only be done if you know what you are doing."
                " cdf will automatically manage the dataset name for you and relies on deterministic naming."
            )

        runtime_context = CONTEXT.get()
        if runtime_context.force_replace:
            write_disposition = "replace"

        return super().run(
            data,
            destination=runtime_context.destination,
            staging=(runtime_context.staging if runtime_context.enable_stage else None),
            dataset_name=dataset_name or runtime_context.dataset_name,
            table_name=table_name,
            write_disposition=write_disposition,
            columns=columns,
            primary_key=primary_key,
            schema=schema,
            loader_file_format=loader_file_format,
            schema_contract=schema_contract,
        )

    @classmethod
    def from_pipeline(cls, pipeline: Pipeline) -> "RuntimePipeline":
        """Creates a RuntimePipeline from a dlt Pipeline object."""
        return cls(
            pipeline.pipeline_name,
            pipelines_dir=pipeline.pipelines_dir,
            pipeline_salt=pipeline.config.pipeline_salt,  # type: ignore[arg-type]
            destination=pipeline.destination,
            staging=pipeline.staging,
            dataset_name=pipeline.dataset_name,
            credentials=None,
            import_schema_path=pipeline._schema_storage_config.import_schema_path,  # type: ignore[arg-type]
            export_schema_path=pipeline._schema_storage_config.export_schema_path,  # type: ignore[arg-type]
            full_refresh=pipeline.full_refresh,
            progress=pipeline.collector,
            must_attach_to_local_pipeline=False,
            config=pipeline.config,
            runtime=pipeline.runtime_config,
        )


def pipeline_factory() -> RuntimePipeline:
    """Creates a cdf pipeline. This is used in lieu of dlt.pipeline. in user code.

    A cdf pipeline is a wrapper around a dlt pipeline that leverages injected information
    from the runtime context. Raises a ValueError if the runtime context is not set.
    """
    runtime = CONTEXT.get()
    options = dict(
        pipeline_name=runtime.pipeline_name,
        destination=runtime.destination,
        staging=runtime.staging if runtime.enable_stage else None,
        dataset_name=runtime.dataset_name,
    )
    # TODO: contribute a PR to expose an _impl_cls argument in dlt.pipeline
    # https://github.com/dlt-hub/dlt/pull/1176
    return RuntimePipeline.from_pipeline(
        pipeline=dlt.pipeline(**options),
    )


@t.overload
def execute_pipeline_specification(
    spec: PipelineSpecification,
    destination: TDestinationReferenceArg,
    staging: t.Optional[TDestinationReferenceArg] = None,
    select: t.Optional[t.List[str]] = None,
    exclude: t.Optional[t.List[str]] = None,
    force_replace: bool = False,
    intercept_sources: t.Literal[False] = False,
    enable_stage: bool = True,
    quiet: bool = False,
) -> M.Result[t.Dict[str, t.Any], Exception]: ...


@t.overload
def execute_pipeline_specification(
    spec: PipelineSpecification,
    destination: TDestinationReferenceArg,
    staging: t.Optional[TDestinationReferenceArg] = None,
    select: t.Optional[t.List[str]] = None,
    exclude: t.Optional[t.List[str]] = None,
    force_replace: bool = False,
    intercept_sources: t.Literal[True] = True,
    enable_stage: bool = True,
    quiet: bool = False,
) -> M.Result[t.Set[dlt.sources.DltSource], Exception]: ...


def execute_pipeline_specification(
    spec: PipelineSpecification,
    destination: TDestinationReferenceArg,
    staging: t.Optional[TDestinationReferenceArg] = None,
    select: t.Optional[t.List[str]] = None,
    exclude: t.Optional[t.List[str]] = None,
    force_replace: bool = False,
    intercept_sources: bool = False,
    enable_stage: bool = True,
    quiet: bool = False,
) -> t.Union[
    M.Result[t.Dict[str, t.Any], Exception],
    M.Result[t.Set[dlt.sources.DltSource], Exception],
]:
    """Executes a pipeline specification."""
    with runtime_context(
        pipeline_name=spec.name,
        dataset_name=spec.dataset_name,
        destination=destination,
        staging=staging,
        select=select,
        exclude=exclude,
        force_replace=force_replace,
        intercept_sources=set() if intercept_sources else None,
        enable_stage=enable_stage and bool(staging),
        applicator=spec.apply,
        metrics=spec.runtime_metrics,
    ):
        context_snapshot = copy_context()
    null = open(os.devnull, "w")
    maybe_redirect = redirect_stdout(null) if quiet else nullcontext()
    if intercept_sources:
        with maybe_redirect:
            context_snapshot.run(spec.main)
        null.close()
        return M.ok(
            t.cast(
                t.Set[dlt.sources.DltSource],
                context_snapshot[CONTEXT].intercept_sources,
            )
        )
    try:
        with maybe_redirect:
            exports = context_snapshot.run(spec.main)
        return M.ok(exports)
    except Exception as e:
        return M.error(e)
    finally:
        null.close()


__all__ = ["pipeline_factory", "execute_pipeline_specification"]
