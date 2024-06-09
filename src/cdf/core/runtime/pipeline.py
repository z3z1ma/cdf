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
import shutil
import typing as t
from contextlib import nullcontext, redirect_stdout, suppress
from pathlib import Path

import dlt
from dlt.common.destination import TDestinationReferenceArg, TLoaderFileFormat
from dlt.common.pipeline import ExtractInfo, LoadInfo, NormalizeInfo
from dlt.common.schema.typing import (
    TAnySchemaColumns,
    TColumnNames,
    TSchemaContract,
    TWriteDisposition,
)
from dlt.extract.extract import Extract, data_to_sources
from dlt.pipeline.exceptions import SqlClientNotAvailable
from dlt.pipeline.pipeline import Pipeline

import cdf.core.context as context
import cdf.core.logger as logger
from cdf.core.specification import PipelineSpecification, SinkSpecification
from cdf.types import M, P

T = t.TypeVar("T")

TPipeline = t.TypeVar("TPipeline", bound=dlt.Pipeline)


def _wrap_pipeline(default_factory: t.Callable[P, TPipeline]):
    """Wraps dlt.pipeline such that it sources the active pipeline from the context."""

    def wrapper(*args: P.args, **kwargs: P.kwargs) -> TPipeline:
        try:
            pipe = context.active_pipeline.get()
            pipe.activate()
            if kwargs:
                logger.warning("CDF runtime detected, ignoring pipeline arguments")
            return t.cast(TPipeline, pipe)
        except LookupError:
            return default_factory(*args, **kwargs)

    return wrapper


pipeline = _wrap_pipeline(dlt.pipeline)
"""Gets the active pipeline or creates a new one with the given arguments."""


def _apply_filters(
    source: dlt.sources.DltSource, resource_patterns: t.List[str], invert: bool
) -> dlt.sources.DltSource:
    """Filters resources in a source based on a list of patterns."""
    return source.with_resources(
        *[
            r
            for r in source.selected_resources
            if any(fnmatch.fnmatch(r, patt) for patt in resource_patterns) ^ invert
        ]
    )


class RuntimePipeline(Pipeline):
    """Overrides certain methods of the dlt pipeline to allow for cdf specific behavior."""

    specification: PipelineSpecification

    def __init__(self, *args: t.Any, **kwargs: t.Any) -> None:
        super().__init__(*args, **kwargs)

        self._force_replace = False
        self._dry_run = False
        self._metric_accumulator = {}
        self._tracked_sources = set()
        self._source_hooks = []

    def configure(
        self,
        dry_run: bool = False,
        force_replace: bool = False,
        select: t.Optional[t.List[str]] = None,
        exclude: t.Optional[t.List[str]] = None,
    ) -> "RuntimePipeline":
        """Configures options which affect the behavior of the pipeline at runtime.

        Args:
            dry_run: Whether to run the pipeline in dry run mode.
            force_replace: Whether to force replace disposition.
            select: A list of glob patterns to select resources.
            exclude: A list of glob patterns to exclude resources.

        Returns:
            RuntimePipeline: The pipeline with source hooks configured.
        """
        S = self.specification

        self._force_replace = force_replace
        self._dry_run = dry_run

        def inject_metrics_and_filters(
            source: dlt.sources.DltSource,
        ) -> dlt.sources.DltSource:
            """Injects metrics and filters into the source."""
            return S.inject_metrics_and_filters(source, self._metric_accumulator)

        def apply_selection(source: dlt.sources.DltSource) -> dlt.sources.DltSource:
            """Applies selection filters to the source."""
            if not select:
                return source
            return _apply_filters(source, select, invert=False)

        def apply_exclusion(source: dlt.sources.DltSource) -> dlt.sources.DltSource:
            """Applies exclusion filters to the source."""
            if not exclude:
                return source
            return _apply_filters(source, exclude, invert=True)

        def apply_feature_flags(source: dlt.sources.DltSource) -> dlt.sources.DltSource:
            """Applies feature flags to the source. User-defined selection takes precedence."""
            if select:
                return source
            return S.workspace.feature_flags.apply_source(source)

        self._source_hooks = [
            inject_metrics_and_filters,
            apply_selection,
            apply_feature_flags,
            apply_exclusion,
        ]
        return self

    @property
    def force_replace(self) -> bool:
        """Whether to force replace disposition."""
        return self._force_replace

    @property
    def dry_run(self) -> bool:
        """Dry run mode."""
        return self._dry_run

    @property
    def metric_accumulator(self) -> t.Mapping[str, t.Any]:
        """A container for accumulating metrics during extract."""
        return self._metric_accumulator

    @property
    def source_hooks(
        self,
    ) -> t.List[t.Callable[[dlt.sources.DltSource], dlt.sources.DltSource]]:
        """The source hooks for the pipeline."""
        return self._source_hooks

    @property
    def tracked_sources(self) -> t.Set[dlt.sources.DltSource]:
        """The sources tracked by the pipeline."""
        return self._tracked_sources

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
        **kwargs: t.Any,
    ) -> ExtractInfo:
        _ = kwargs
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

        for i, source in enumerate(sources):
            for hook in self._source_hooks:
                sources[i] = hook(source)
            self._tracked_sources.add(source)

        if self.dry_run:
            return self._get_step_info(
                step=Extract(
                    self._schema_storage,
                    self._normalize_storage_config(),
                    self.collector,
                    original_data=data,
                )
            )

        if self.force_replace:
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

        if self.metric_accumulator:
            logger.info(
                "Metrics captured during %s extract, sideloading to destination...",
                info.pipeline.pipeline_name,
            )
            super().extract(
                dlt.resource(
                    [
                        {
                            "load_id": load_id,
                            "metrics": dict(self.metric_accumulator),
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

        if self.specification.persist_extract_package:
            logger.info(
                "Persisting extract package for %s...", info.pipeline.pipeline_name
            )
            for package in info.load_packages:
                # TODO: move this to a top-level function
                root = Path(self.pipelines_dir)
                base = Path(package.package_path).relative_to(root)
                path = shutil.make_archive(
                    base_name=package.load_id,
                    format="gztar",
                    root_dir=root,
                    base_dir=base,
                    logger=logger,
                )
                logger.info("Extract package staged at %s", path)
                target = f"extracted/{package.load_id}.tar.gz"
                self.specification.workspace.filesystem.put(path, target)
                logger.info("Package uploaded to %s using project fs", target)
                Path(path).unlink()
                logger.info("Cleaned up staged package")
                # TODO: listing and manipulating these should be first-class
                # this will enable us to "replay" a pipeline
                # logger.info(self.specification.workspace.filesystem.ls("extracted"))

        return info

    def normalize(
        self,
        workers: int = 1,
        loader_file_format: TLoaderFileFormat = None,  # type: ignore[arg-type]
    ) -> NormalizeInfo:
        return super().normalize(workers, loader_file_format)

    def run(
        self,
        data: t.Any = None,
        *,
        table_name: str = None,  # type: ignore[arg-type]
        write_disposition: TWriteDisposition = None,  # type: ignore[arg-type]
        columns: TAnySchemaColumns = None,  # type: ignore[arg-type]
        primary_key: TColumnNames = None,  # type: ignore[arg-type]
        schema: dlt.Schema = None,  # type: ignore[arg-type]
        loader_file_format: TLoaderFileFormat = None,  # type: ignore[arg-type]
        schema_contract: TSchemaContract = None,  # type: ignore[arg-type]
        **kwargs: t.Any,
    ) -> LoadInfo:
        _ = kwargs
        if self._force_replace:
            write_disposition = "replace"

        return super().run(
            data,
            table_name=table_name,
            write_disposition=write_disposition,
            columns=columns,
            primary_key=primary_key,
            schema=schema,
            loader_file_format=loader_file_format,
            schema_contract=schema_contract,
        )


class PipelineResult(t.NamedTuple):
    """The result of executing a pipeline specification."""

    exports: t.Dict[str, t.Any]
    pipeline: RuntimePipeline


def execute_pipeline_specification(
    pipe_spec: PipelineSpecification,
    sink_spec: t.Union[
        TDestinationReferenceArg,
        t.Tuple[TDestinationReferenceArg, t.Optional[TDestinationReferenceArg]],
        SinkSpecification,
    ],
    select: t.Optional[t.List[str]] = None,
    exclude: t.Optional[t.List[str]] = None,
    force_replace: bool = False,
    dry_run: bool = False,
    enable_stage: bool = True,
    quiet: bool = False,
    **pipeline_options: t.Any,
) -> M.Result[PipelineResult, Exception]:
    """Executes a pipeline specification.

    Args:
        pipe_spec: The pipeline specification.
        sink_spec: The destination where the pipeline will write data.
        select: A list of glob patterns to select resources.
        exclude: A list of glob patterns to exclude resources.
        force_replace: Whether to force replace disposition.
        dry_run: Whether to run the pipeline in dry run mode.
        enable_stage: Whether to enable staging. If disabled, staging will be ignored.
        quiet: Whether to suppress output.
        pipeline_options: Additional dlt.pipeline constructor arguments.

    Returns:
        M.Result[PipelineResult, Exception]: The result of executing the pipeline specification.
    """
    if isinstance(sink_spec, SinkSpecification):
        destination, staging = sink_spec.get_ingest_config()
    elif isinstance(sink_spec, tuple):
        destination, staging = sink_spec
    else:
        destination, staging = sink_spec, None

    pipeline_options.update(
        {"destination": destination, "staging": staging if enable_stage else None}
    )
    pipe_reference = pipe_spec.create_pipeline(
        RuntimePipeline, **pipeline_options
    ).configure(dry_run, force_replace, select, exclude)
    token = context.active_pipeline.set(pipe_reference)

    null = open(os.devnull, "w")
    maybe_redirect = redirect_stdout(null) if quiet else nullcontext()
    try:
        with maybe_redirect:
            result = PipelineResult(exports=pipe_spec(), pipeline=pipe_reference)
        if dry_run:
            return M.ok(result)
        with (
            suppress(KeyError, SqlClientNotAvailable),
            pipe_reference.sql_client() as client,
            client.with_staging_dataset(staging=True) as client_staging,
        ):
            strategy = dlt.config["destination.replace_strategy"]
            if strategy in ("insert-from-staging",) and client_staging.has_dataset():
                logger.info(
                    f"Cleaning up staging dataset {client_staging.dataset_name}"
                )
                client_staging.drop_dataset()
        return M.ok(result)
    except Exception as e:
        return M.error(e)
    finally:
        context.active_pipeline.reset(token)
        null.close()


__all__ = ["execute_pipeline_specification"]
