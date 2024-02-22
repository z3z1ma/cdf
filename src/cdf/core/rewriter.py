"""This module contains the logic for rewriting the ast of a python script.

Headers provide the ability to adjust the behavior of scripts without modifying the original source code. This
is achieved through dynamic wrapping of the upstream interface.

The headers are extracted from the function bodies for top-level insertion into scripts. Return values and
function parameters are stripped from the function bodies during conversion to headers. This confers the
significant benefit of having fully functional and testable wrappers for the pipeline constructor outside of
the rewriting mechanism. In a generalized sense, this takes decorators which operate on a function
and unnests the scope such that it operates on a local variable of the same name as the function argument.
"""
from __future__ import annotations

import ast
import inspect
import types
import typing as t
from copy import deepcopy

import cdf.core.constants as c
import cdf.core.exceptions as ex
from cdf.core.monads import Err, Ok, Result

if t.TYPE_CHECKING:
    import sqlmesh
    from dlt.pipeline.pipeline import Pipeline

    PipeFactory = t.Callable[..., Pipeline]
    SinkStruct = t.Tuple[str, t.Any, t.Any] | t.Callable[[], t.Tuple[str, t.Any, t.Any]]


class RewriteError(ex.CDFError):
    """An error raised when rewriting fails."""


def _to_header(
    func: t.Callable,
    prepends: t.List[ast.Module] | None = None,
    appends: t.List[ast.Module] | None = None,
) -> ast.Module:
    """Converts a function's body to an ast node which can be used as a header that modifies behavior"""
    source = inspect.getsource(func)
    tree = ast.parse(source)
    func_def = t.cast(ast.FunctionDef, tree.body[0])
    header = func_def.body[:-1]
    for p in reversed(prepends or []):
        header = [*p.body, *header]
    for a in appends or []:
        header.extend(a.body)
    return ast.Module(body=header)


# The following functions serve 2 purposes:
# 1. They provide wrappers which can be used to modify the behavior of the dlt.pipeline instance.
# 2. They are converted to headers which mutate the behavior of the pipeline constructor when prepended to a script.


def get_entrypoint_ref() -> "PipeFactory":
    """
    Provides a canonical reference for the dlt.pipeline constructor.

    This is used by all other wrappers to ensure a consistent entrypoint for the pipeline constructor and to provide
    a separate reference for wrapping. This separate reference allows us to run rewritten pipelines safely across
    threads or long running processes since we avoid assignments to the global scope. As such, it is added to the top
    of the script by the rewriter by default.
    """
    from dlt import pipeline as __entrypoint__

    return __entrypoint__


def inject_source_capture(__entrypoint__: "PipeFactory") -> "PipeFactory":
    """
    Overwrites the extract method of a pipeline to capture the sources and return an empty list.

    This causes the pipeline to be executed in a dry-run mode essentially while sources are captured
    and returned to the caller. Pipelines scripts remain valid via this mechanism.
    """
    import functools

    from dlt.extract.extract import data_to_sources

    import cdf.core.constants as c

    L = locals()
    L[c.SOURCE_CONTAINER] = container = set()

    def __wrap__(__pipefunc__: "PipeFactory"):
        @functools.wraps(__pipefunc__)
        def wrapper(*args, **kwargs):
            pipe = __pipefunc__(*args, **kwargs)

            @functools.wraps(pipe.extract)
            def _extract(data, **kwargs):
                with pipe._maybe_destination_capabilities():
                    forward = kwargs.copy()
                    for ignore in ("workers", "max_parallel_items"):
                        forward.pop(ignore, None)
                    sources = data_to_sources(data, pipe, **forward)
                for source in sources:
                    container.add(source)
                return []

            setattr(pipe, "extract", _extract)
            return pipe

        return wrapper

    __entrypoint__ = __wrap__(__entrypoint__)
    return __entrypoint__


def inject_destination_parametrization(
    __entrypoint__: "PipeFactory", sink: "SinkStruct" = ("duckdb", None, None)
) -> "PipeFactory":
    """
    Parameterizes destination via wrapping a nonlocal `sink` var and overriding the destination parameter.

    The parameter is overridden in the `run` and `load` methods of the pipeline. The `sink` variable is expected
    to be a tuple of the form (destination, staging, gateway) and is expected to be injected via another header.
    """
    import functools
    import inspect

    if callable(sink):
        sink = sink()

    def __wrap__(__pipefunc__: "PipeFactory"):
        __unwrapped__ = inspect.unwrap(__pipefunc__)
        spec = inspect.getfullargspec(__unwrapped__)

        @functools.wraps(__pipefunc__)
        def wrapper(*args, **kwargs):
            ckwargs = inspect.getcallargs(__unwrapped__, *args, **kwargs)
            ckwargs["destination"] = sink[0]
            ckwargs["staging"] = sink[1]
            cargs = [ckwargs.pop(k) for k in spec.args if k in ckwargs]
            if spec.varargs:
                cargs.extend(ckwargs.pop(spec.varargs, []))
            if spec.varkw:
                ckwargs.update(ckwargs.pop(spec.varkw, {}))
            pipe = __pipefunc__(*cargs, **ckwargs)
            run = pipe.run

            @functools.wraps(run)
            def _run(data, **kwargs):
                kwargs["destination"] = sink[0]
                kwargs["staging"] = sink[1]
                return run(data, **kwargs)

            setattr(pipe, "run", _run)
            load = pipe.load

            @functools.wraps(load)
            def _load(destination=None, dataset_name=None, credentials=None, **kwargs):
                return load(sink[0], dataset_name, credentials, **kwargs)  # type: ignore

            setattr(pipe, "load", _load)
            return pipe

        return wrapper

    __entrypoint__ = __wrap__(__entrypoint__)
    return __entrypoint__


def inject_duckdb_destination() -> "SinkStruct":
    """Minimum via sink definition for duckdb destination."""
    sink = ("duckdb", None, None)
    return sink


def inject_resource_selection(
    __entrypoint__: "PipeFactory", *resource_patterns: str
) -> "PipeFactory":
    """Filters resources in the extract method of a pipeline based on a list of patterns."""
    import fnmatch
    import functools

    from dlt.extract.extract import data_to_sources

    def __wrap__(__pipefunc__: "PipeFactory"):
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
                        ]
                    )
                return extract(data, **kwargs)

            setattr(pipe, "extract", _extract)
            return pipe

        return wrapper

    __entrypoint__ = __wrap__(__entrypoint__)
    return __entrypoint__


def inject_replace_disposition(__entrypoint__: "PipeFactory") -> "PipeFactory":
    """
    Ignores state and truncates the destination table before loading.

    Schema inference history is preserved. This is standard dlt behavior. This is aimed at reloading
    data in existing tables given a long term support pipeline with a stable schema and potential
    upstream mutability. To clear schema inference data, drop the target dataset instead.
    """
    import functools

    def __wrap__(__pipefunc__: "PipeFactory"):
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


def inject_feature_flags(__entrypoint__: "PipeFactory") -> "PipeFactory":
    """Wraps the pipeline with feature flagging."""
    import functools

    from dlt.extract.extract import data_to_sources

    from cdf.core.context import active_workspace
    from cdf.core.feature_flags import create_harness_provider

    ff = create_harness_provider()

    def __wrap__(__pipefunc__: "PipeFactory"):
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


noop = ast.parse("pass")
add_entrypoint = _to_header(get_entrypoint_ref)
capture_sources = _to_header(inject_source_capture)
set_duckdb_destination = _to_header(inject_duckdb_destination)
parametrize_destination = _to_header(inject_destination_parametrization)
set_basic_destination = lambda d="duckdb": ast.parse(  # noqa
    f"sink=({d!r}, None, None)"
)
filter_resources = lambda *patts: _to_header(  # noqa
    inject_resource_selection, prepends=[ast.parse(f"resource_patterns = {patts!r}")]
)
force_replace_disposition = _to_header(inject_replace_disposition)
apply_feature_flags = _to_header(inject_feature_flags)
anchor_imports = lambda mod: ast.parse(f"__package__ = {mod!r}")  # noqa
add_debugger = _to_header(setup_debugger)
assert_recent_intervals = _to_header(
    raise_on_missing_intervals,
    prepends=[
        ast.parse("import cdf"),
        ast.parse("context = cdf.current_context()"),
        ast.parse("spec = cdf.current_spec()"),
    ],
)


def create_rewriter(root: str) -> ast.NodeTransformer:
    """Creates an import rewriter class with a custom root module."""

    def _substitute_entrypoint(self, node: ast.Call) -> ast.Call:
        if isinstance(node.func, ast.Attribute):
            if isinstance(node.func.value, ast.Name) and node.func.value.id == "dlt":
                if node.func.attr == "pipeline":
                    new = ast.copy_location(
                        ast.Call(
                            func=ast.Name(id="__entrypoint__", ctx=ast.Load()),
                            args=node.args,
                            keywords=node.keywords,
                        ),
                        node,
                    )
                    return self.generic_visit(new)
        return node

    overrides: dict = {}
    if root == c.PIPELINES:
        overrides["visit_Call"] = _substitute_entrypoint

    return type(
        "Rewriter",
        (ast.NodeTransformer,),
        overrides,
    )()


rewriters = {
    k: create_rewriter(k)
    for k in (
        c.PIPELINES,
        c.PUBLISHERS,
        c.SINKS,
        c.SCRIPTS,
    )
}


def rewrite_script(
    tree: ast.Module, comp_type: str, *headers: ast.Module, copy: bool = True
) -> Result[str, ex.CDFError]:
    """
    Generates code from a python script ast with additional headers.

    Headers are prepended to the script and provide the ability to adjust the behavior of scripts
    without modifying the original source code.

    Args:
        tree: The ast of a python script.
        comp_type: The type of the component.
        headers: Additional ast nodes to prepend to the tree in the order they are provided.
        copy: Whether to deepcopy the tree before rewriting. Defaults to True.

    Returns:
        str: The transformed code of the python script.
    """
    try:
        tree = deepcopy(tree) if copy else tree
        for header in reversed(headers):
            tree.body = [*header.body, *tree.body]
        if comp_type == c.PIPELINES:
            tree.body = [*add_entrypoint.body, *tree.body]
        stringified_code = ast.unparse(
            ast.fix_missing_locations(rewriters[comp_type].visit(tree))
        )
        return Ok(
            "\n".join(line for line in stringified_code.splitlines() if line.strip())
        )
    except Exception as e:
        return Err(RewriteError(f"Failed to rewrite pipeline: {e}"))
