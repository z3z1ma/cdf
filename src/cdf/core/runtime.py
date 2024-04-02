import fnmatch
import typing as t

import dlt
from dlt.common.destination import TLoaderFileFormat
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


class CDFPipeline(Pipeline):
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

        execution_context = context.execution_context.get()
        if execution_context.intercept_sources is not None:
            extract_step = Extract(
                self._schema_storage,
                self._normalize_storage_config(),
                self.collector,
                original_data=data,
            )
            for source in sources:
                execution_context.intercept_sources.add(source)
            return self._get_step_info(extract_step)

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

        if execution_context.select:
            for i, source in enumerate(sources):
                sources[i] = _apply_filters(
                    source, execution_context.select, invert=False
                )
        else:
            active_project = context.active_project.get()
            for i, source in enumerate(sources):
                sources[i] = active_project.feature_flag_provider(source)

        if execution_context.exclude:
            for i, source in enumerate(sources):
                sources[i] = _apply_filters(
                    source, execution_context.exclude, invert=True
                )

        for i, source in enumerate(sources):
            sources[i] = execution_context.applicator(source)

        if execution_context.force_replace:
            write_disposition = "replace"

        return super().extract(
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

        execution_context = context.execution_context.get()
        if execution_context.force_replace:
            write_disposition = "replace"

        return super().run(
            data,
            destination=execution_context.destination,
            staging=(
                execution_context.staging if execution_context.enable_stage else None
            ),
            dataset_name=dataset_name or execution_context.dataset_name,
            table_name=table_name,
            write_disposition=write_disposition,
            columns=columns,
            primary_key=primary_key,
            schema=schema,
            loader_file_format=loader_file_format,
            schema_contract=schema_contract,
        )

    @classmethod
    def from_pipeline(cls, pipeline: Pipeline) -> "CDFPipeline":
        """Creates a CDFPipeline from a Pipeline."""
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


PipelineFactory = t.Callable[..., Pipeline]


def pipeline() -> CDFPipeline:
    execution_context = context.execution_context.get()
    pipe_options: dict = {"pipeline_name": execution_context.pipeline_name}
    for k, v in (
        ("destination", execution_context.destination),
        ("dataset_name", execution_context.dataset_name),
        ("staging", execution_context.staging),
    ):
        if v is not None:
            pipe_options[k] = v

    # lets set pipelines_dir internally?
    # lets standardize import_schema_path and export_schema_path leveraging fsspec

    return CDFPipeline.from_pipeline(pipeline=dlt.pipeline(**pipe_options))


__all__ = ["pipeline"]
