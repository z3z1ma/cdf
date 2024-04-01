import fnmatch
import functools
import typing as t

import dlt
from dlt.extract.extract import data_to_sources
from dlt.pipeline.pipeline import Pipeline

import cdf.core.constants as c
import cdf.core.context as context
import cdf.core.logger as logger

PipelineFactory = t.Callable[..., Pipeline]


def pipeline(
    dataset_name: t.Optional[str] = None,
    pipelines_dir: t.Optional[str] = None,
    import_schema_path: t.Optional[str] = None,
    export_schema_path: t.Optional[str] = None,
) -> Pipeline:
    if dataset_name is not None:
        logger.warning(
            "Using dataset_name is a cdf pipeline should only be done if you know what you are doing."
            " cdf will automatically manage the dataset name for you and relies on deterministic naming."
        )

    execution_context = context.execution_context.get()
    pipe_options = {"pipeline_name": execution_context.pipeline_name}
    for k, v in (
        ("destination", execution_context.destination),
        ("staging", execution_context.staging),
        ("progress", execution_context.progress),
        ("dataset_name", dataset_name),
        ("pipelines_dir", pipelines_dir),
        ("import_schema_path", import_schema_path),
        ("export_schema_path", export_schema_path),
    ):
        if v is not None:
            pipe_options[k] = v

    pipe = dlt.pipeline(**pipe_options)
    # TODO: is this patching necessary?
    # would we be better off subclassing pipeline, and copy/pasting the factory body? [lets do this in the AM]
    # perhaps we can contribute a PR so the factory method takes a class to instantiate...

    if execution_context.intercept_sources:
        # Overwrites the extract method of a pipeline to capture the sources and return an empty list.
        # This causes the pipeline to be executed in a dry-run mode essentially while sources are captured
        # and returned to the caller. Pipelines scripts remain valid via this mechanism.
        _container = set()

        @functools.wraps(pipe.extract)
        def _interception(data, **kwargs):
            with pipe._maybe_destination_capabilities():
                forward = kwargs.copy()
                for ignore in ("workers", "max_parallel_items"):
                    forward.pop(ignore, None)
                sources = data_to_sources(data, pipe, **forward)
            for source in sources:
                _container.add(source)
            context.intercepted_sources.set(_container)
            return []

        setattr(pipe, "extract", _interception)

    if execution_context.select or execution_context.exclude:
        # Overwrites the extract method of a pipeline to filter sources based on select and exclude patterns.
        # This allows for selective extraction of resources from a pipeline script.
        _select = execution_context.select or ["*"]
        _exclude = execution_context.exclude or []

        @functools.wraps(pipe.extract)
        def _selection(data, **kwargs):
            with pipe._maybe_destination_capabilities():
                forward = kwargs.copy()
                for ignore in ("workers", "max_parallel_items"):
                    forward.pop(ignore, None)
                sources = data_to_sources(data, pipe, **forward)
            for source in sources:
                for name, resource in source.resources.items():
                    if not any(fnmatch.fnmatch(name, pattern) for pattern in _select):
                        del source.resources[name]
                    if any(fnmatch.fnmatch(name, pattern) for pattern in _exclude):
                        del source.resources[name]

        setattr(pipe, "extract", _selection)

    return pipe


def inject_resource_selection(
    __entrypoint__: PipelineFactory, *resource_patterns: str, invert: bool = False
) -> PipelineFactory:
    """Filters resources in the extract method of a pipeline based on a list of patterns."""
    import fnmatch
    import functools

    from dlt.extract.extract import data_to_sources

    def __wrap__(__pipefunc__: PipelineFactory):
        @functools.wraps(__pipefunc__)
        def wrapper(*args, **kwargs):
            pipe = __pipefunc__(*args, **kwargs)
            extract = pipe.extract

            @functools.wraps(extract)
            def _extract(data, **kwargs):
                with pipe._maybe_destination_capabilities():
                    fwd = kwargs.copy()
                    fwd.pop("workers", None)
                    fwd.pop("max_parallel_items", None)
                    data = data_to_sources(data, pipe, **fwd)
                for i, source in enumerate(data):
                    data[i] = source.with_resources(
                        *[
                            r
                            for r in source.selected_resources
                            if any(
                                fnmatch.fnmatch(r, patt) for patt in resource_patterns
                            )
                            ^ invert
                        ]
                    )
                return extract(data, **kwargs)

            setattr(pipe, "extract", _extract)
            return pipe

        return wrapper

    __entrypoint__ = __wrap__(__entrypoint__)
    return __entrypoint__


def inject_replace_disposition(__entrypoint__: PipelineFactory) -> PipelineFactory:
    """
    Ignores state and truncates the destination table before loading.
    Schema inference history is preserved. This is standard dlt behavior. This is aimed at reloading
    data in existing tables given a long term support pipeline with a stable schema and potential
    upstream mutability. To clear schema inference data, drop the target dataset instead.
    """
    import functools

    def __wrap__(__pipefunc__: PipelineFactory):
        @functools.wraps(__pipefunc__)
        def wrapper(*args, **kwargs):
            pipe = __pipefunc__(*args, **kwargs)
            run = pipe.run

            @functools.wraps(run)
            def _run(data, **kwargs):
                kwargs["write_disposition"] = "replace"
                return run(data, **kwargs)

            setattr(pipe, "run", _run)
            extract = pipe.extract

            @functools.wraps(extract)
            def _extract(data, **kwargs):
                kwargs["write_disposition"] = "replace"
                return extract(data, **kwargs)

            setattr(pipe, "extract", _extract)
            return pipe

        return wrapper

    __entrypoint__ = __wrap__(__entrypoint__)
    return __entrypoint__


def inject_feature_flags(__entrypoint__: PipelineFactory) -> PipelineFactory:
    """Wraps the pipeline with feature flagging."""
    import functools

    from dlt.extract.extract import data_to_sources

    from cdf.core.context import active_workspace
    from cdf.core.feature_flags import create_provider

    ff = create_provider()

    def __wrap__(__pipefunc__: PipelineFactory):
        @functools.wraps(__pipefunc__)
        def wrapper(*args, **kwargs):
            pipe = __pipefunc__(*args, **kwargs)
            extract = pipe.extract

            @functools.wraps(extract)
            def _extract(data, **kwargs):
                with pipe._maybe_destination_capabilities():
                    fwd = kwargs.copy()
                    fwd.pop("workers", None)
                    fwd.pop("max_parallel_items", None)
                    data = data_to_sources(data, pipe, **fwd)
                for i, source in enumerate(data):
                    data[i] = ff(source, active_workspace.get())
                return extract(data, **kwargs)

            setattr(pipe, "extract", _extract)
            return pipe

        return wrapper

    __entrypoint__ = __wrap__(__entrypoint__)
    return __entrypoint__


def setup_debugger() -> t.Callable[[t.Any, t.Any, t.Any], None]:
    """Installs a post-mortem debugger for unhandled exceptions."""
    import pdb
    import sys
    import traceback

    def debug_hook(etype, value, tb):
        traceback.print_exception(etype, value, tb)
        pdb.post_mortem(tb)

    sys.excepthook = debug_hook
    return debug_hook


def raise_on_missing_intervals(
    context: "sqlmesh.Context", spec: types.SimpleNamespace
) -> None:
    """Checks for missing intervals in tracked dependencies based on depends_on attribute of the spec."""
    import datetime

    from sqlmesh.core.dialect import normalize_model_name

    import cdf.core.logger as logger

    if hasattr(spec, "depends_on"):
        models = context.models
        for dependency in spec.depends_on:
            normalized_name = normalize_model_name(
                dependency, context.default_catalog, context.default_dialect
            )
            if normalized_name not in models:
                raise ValueError(
                    f"Cannot find tracked dependency {dependency} in models."
                )
            model = models[normalized_name]
            snapshot = context.get_snapshot(normalized_name)
            assert snapshot, f"Snapshot not found for {normalized_name}"
            if snapshot.missing_intervals(
                datetime.date.today() - datetime.timedelta(days=7),
                datetime.date.today() - datetime.timedelta(days=1),
            ):
                raise ValueError(
                    f"Model {model} has missing intervals. Cannot publish."
                )
            logger.info(f"Model {model} has no missing intervals.")
        logger.info("All tracked dependencies passed interval check.")
    else:
        logger.warn("No tracked dependencies found in spec. Skipping interval check.")
    return None


__all__ = ["pipeline"]
