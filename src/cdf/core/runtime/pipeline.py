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
import types
import typing as t
from contextlib import nullcontext, redirect_stdout, suppress

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
P = t.ParamSpec("P")

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

    def __init__(self, *args: t.Any, **kwargs: t.Any) -> None:
        super().__init__(*args, **kwargs)
        self._force_replace = False
        self._dry_run = False
        self._runtime_metrics = {}
        self._tracked_sources = set()
        self._source_hooks = []

    def configure_force_replace(self, force_replace: bool) -> "RuntimePipeline":
        """Sets the force replace disposition."""
        self._force_replace = force_replace
        return self

    def configure_dry_run(self, dry_run: bool) -> "RuntimePipeline":
        """Sets the dry run mode."""
        self._dry_run = dry_run
        return self

    def attach_metric_container(
        self, container: types.MappingProxyType[str, t.Any]
    ) -> "RuntimePipeline":
        """Attaches a container for sideloading captured metrics during extract."""
        self._runtime_metrics = container
        return self

    def configure_source_hooks(
        self,
        *hooks: t.Callable[[dlt.sources.DltSource], dlt.sources.DltSource],
        extend: bool = False,
    ) -> "RuntimePipeline":
        """Sets the source hooks for the pipeline."""
        if extend:
            self._source_hooks.extend(hooks)
        else:
            self._source_hooks = list(hooks)
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
    def runtime_metrics(self) -> t.Mapping[str, t.Any]:
        """A container for captured metrics during extract."""
        return self._runtime_metrics

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

        if self.runtime_metrics:
            logger.info(
                "Metrics captured during %s extract, sideloading to destination...",
                info.pipeline.pipeline_name,
            )
            super().extract(
                dlt.resource(
                    [
                        {
                            "load_id": load_id,
                            "metrics": dict(self.runtime_metrics),
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
        table_name: str = None,  # type: ignore[arg-type]
        write_disposition: TWriteDisposition = None,  # type: ignore[arg-type]
        columns: TAnySchemaColumns = None,  # type: ignore[arg-type]
        primary_key: TColumnNames = None,  # type: ignore[arg-type]
        schema: dlt.Schema = None,  # type: ignore[arg-type]
        loader_file_format: TLoaderFileFormat = None,  # type: ignore[arg-type]
        schema_contract: TSchemaContract = None,  # type: ignore[arg-type]
    ) -> LoadInfo:
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
    spec: PipelineSpecification,
    destination: TDestinationReferenceArg,
    staging: t.Optional[TDestinationReferenceArg] = None,
    select: t.Optional[t.List[str]] = None,
    exclude: t.Optional[t.List[str]] = None,
    force_replace: bool = False,
    dry_run: bool = False,
    enable_stage: bool = True,
    quiet: bool = False,
    metric_container: t.Optional[t.MutableMapping[str, t.Any]] = None,
    **pipeline_options: t.Any,
) -> M.Result[PipelineResult, Exception]:
    """Executes a pipeline specification."""

    metric_container = metric_container or {}
    pipeline_options.update(
        {"destination": destination, "staging": staging if enable_stage else None}
    )

    hooks = [lambda source: spec.inject_metrics_and_filters(source, metric_container)]
    if select:
        hooks.append(lambda source: _apply_filters(source, select, invert=False))
    else:
        proj = context.active_project.get()
        hooks.append(lambda source: proj.feature_flag_provider.apply_source(source))
    if exclude:
        hooks.append(lambda source: _apply_filters(source, exclude, invert=True))

    pipe_reference = (
        spec.create_pipeline(RuntimePipeline, **pipeline_options)
        .attach_metric_container(types.MappingProxyType(metric_container))
        .configure_dry_run(dry_run)
        .configure_source_hooks(*hooks)
        .configure_force_replace(force_replace)
    )
    token = context.active_pipeline.set(pipe_reference)

    null = open(os.devnull, "w")
    maybe_redirect = redirect_stdout(null) if quiet else nullcontext()
    try:
        with maybe_redirect:
            result = PipelineResult(exports=spec(), pipeline=pipe_reference)
        if dry_run:
            return M.ok(result)
        with (
            suppress(KeyError),
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
