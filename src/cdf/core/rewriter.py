"""This module contains the logic for rewriting the ast of a python script.

Headers provide the ability to adjust the behavior of scripts without modifying the original source code. This
is achieved through dynamic wrapping of the upstream interface.

The headers are extracted from the function bodies for top-level insertion into scripts. Return values and
function parameters are stripped from the function bodies during conversion to headers. This confers the
significant benefit of having fully functional and testable wrappers for the pipeline constructor outside of
the rewriting mechanism.
"""
import ast
import inspect
import typing as t
from copy import deepcopy

import cdf.core.constants as c
import cdf.core.exceptions as ex
from cdf.core.monads import Err, Ok, Result

if t.TYPE_CHECKING:
    from dlt.pipeline.pipeline import Pipeline

    PipeFactory = t.Callable[..., Pipeline]
    SimpleSink = t.Tuple[str, t.Any, t.Any]


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
    for p in prepends or []:
        header = [*p.body, *header]
    for a in appends or []:
        header.extend(a.body)
    return ast.Module(body=header)


# fmt: off
# ruff: noqa: E401 E702 F401 F823 F841

# We want this code to be injected with minimal changes to the original source
# code so we keep it compact and disable black formatting for this section.
def get_entrypoint() -> "PipeFactory":
    """Provides a canonical entrypoint for the dlt.pipeline constructor.

    This is used by all other wrappers to ensure a consistent entrypoint for the pipeline constructor and to provide
    a separate reference for wrapping. This separate reference allows us to run rewritte pipelines safely across
    threads or long running processes. As such, it is added to the top of the script by the rewriter by default.

    The rewriter also replaces all calls to `dlt.pipeline` with the `__entrypoint__` function. Any fancy calls
    to `dlt.pipeline` that are not direct calls will not be rewritten and will not be wrapped. This is a limitation
    of the rewriter and is not expected to be a problem in practice. This decision lets us keep complexity very low.
    """
    from dlt import pipeline as __entrypoint__
    return __entrypoint__

def source_capture_wrapper(__entrypoint__: "PipeFactory") -> "PipeFactory":
    """Overwrites the extract method of a pipeline to capture the sources and return an empty list.

    This causes the pipeline to be executed in a dry-run mode essentially while sources are captured
    and returned to the caller. Pipelines scripts remain valid via this mechanism.
    """
    import functools, cdf.core.constants as c # isort:skip
    from dlt.extract.extract import data_to_sources
    locals()[c.SOURCE_CONTAINER] = container = set()
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
            pipe.extract = _extract # type: ignore
            return pipe
        return wrapper
    __entrypoint__ = __wrap__(__entrypoint__)
    return __entrypoint__

def parametrized_destination_wrapper(__entrypoint__: "PipeFactory", sink: "SimpleSink" = ("duckdb", None, None)) -> "PipeFactory":
    """Parameterizes destination via wrapping a nonlocal `sink` var and overriding the destination parameter. 

    The parameter is overridden in the `run` and `load` methods of the pipeline. The `sink` variable is expected
    to be a tuple of the form (destination, staging, gateway) and is expected to be injected via another header.
    """
    import functools, inspect # isort:skip
    def __wrap__(__pipefunc__: "PipeFactory"):
        __unwrapped__ = inspect.unwrap(__pipefunc__)
        spec = inspect.getfullargspec(__unwrapped__)
        @functools.wraps(__pipefunc__)
        def wrapper(*args, **kwargs):
            ckwargs = inspect.getcallargs(__unwrapped__, *args, **kwargs)
            ckwargs["destination"] = sink[0]; ckwargs["staging"] = sink[1]
            cargs = [ckwargs.pop(k) for k in spec.args if k in ckwargs]
            if spec.varargs:
                cargs.extend(ckwargs.pop(spec.varargs, []))
            if spec.varkw:
                ckwargs.update(ckwargs.pop(spec.varkw, {}))
            pipe = __pipefunc__(*cargs, **ckwargs)
            run = pipe.run
            @functools.wraps(run)
            def _run(data, **kwargs):
                kwargs["destination"] = sink[0]; kwargs["staging"] = sink[1]
                return run(data, **kwargs)
            pipe.run = _run # type: ignore
            load = pipe.load
            @functools.wraps(load)
            def _load(destination=None, dataset_name=None, credentials=None, **kwargs):
                return load(sink[0], dataset_name, credentials, **kwargs) # type: ignore
            pipe.load = _load
            return pipe
        return wrapper
    __entrypoint__ = __wrap__(__entrypoint__)
    return __entrypoint__

def get_duckdb_destination() -> "SimpleSink":
    """Minimum via sink definition for duckdb destination."""
    sink=("duckdb", None, None)
    return sink

def resource_filter_wrapper(__entrypoint__: "PipeFactory", *resource_patterns: str) -> "PipeFactory":
    """Filters resources in the extract method of a pipeline based on a list of patterns."""
    import functools, fnmatch # isort:skip
    from dlt.extract.extract import data_to_sources
    def __wrap__(__pipefunc__: "PipeFactory"):
        @functools.wraps(__pipefunc__)
        def wrapper(*args, **kwargs):
            pipe = __pipefunc__(*args, **kwargs)
            extract = pipe.extract
            @functools.wraps(extract)
            def _extract(data, **kwargs):
                with pipe._maybe_destination_capabilities():
                    forward = kwargs.copy()
                    forward.pop("workers", None)
                    forward.pop("max_parallel_items", None)
                    data = data_to_sources(data, pipe, **forward)
                for i, source in enumerate(data):
                    data[i] = source.with_resources(*[
                        r for r in source.selected_resources
                        if any(fnmatch.fnmatch(r, patt) for patt in resource_patterns)
                    ])
                return extract(data, **kwargs)
            pipe.extract = _extract
            return pipe
        return wrapper
    __entrypoint__ = __wrap__(__entrypoint__)
    return __entrypoint__

def replace_disposition_wrapper(__entrypoint__: "PipeFactory") -> "PipeFactory":
    """Ignores state and truncates the destination table before loading.

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
            pipe.run = _run # type: ignore
            extract = pipe.extract
            @functools.wraps(extract)
            def _extract(data, **kwargs):
                kwargs["write_disposition"] = "replace"
                return extract(data, **kwargs)
            pipe.extract = _extract
            return pipe
        return wrapper
    __entrypoint__ = __wrap__(__entrypoint__)
    return __entrypoint__


def feature_flag_wrapper(__entrypoint__: "PipeFactory") -> "PipeFactory":
    """Wraps the pipeline with feature flagging.

    This is a placeholder for future use. It is not currently implemented.
    """
    pass

    return __entrypoint__
# End of injectable function bodies

# fmt: on


# Wrappers are converted to headers for use in the rewriter
# These headers are executed eagerly in the top-level scope
noop_header = ast.parse("pass")
entrypoint_header = _to_header(get_entrypoint)
source_capture_header = _to_header(source_capture_wrapper)
duckdb_destination_header = _to_header(get_duckdb_destination)
parametrized_destination_header = _to_header(parametrized_destination_wrapper)
basic_destination_header = lambda d="duckdb": ast.parse(  # noqa
    f"sink=({d!r}, None, None)"
)
resource_filter_header = lambda *patts: _to_header(  # noqa
    resource_filter_wrapper, prepends=[ast.parse(f"resource_patterns = {patts!r}")]
)
replace_disposition_header = _to_header(replace_disposition_wrapper)
feature_flag_header = _to_header(feature_flag_wrapper)


def create_rewriter(root: str) -> ast.NodeTransformer:
    """Creates an import rewriter class with a custom root module."""

    def _reroot(_, node: ast.ImportFrom) -> ast.ImportFrom:
        if node.level >= 1:
            node.module = f"{root}.{node.module}"
            node.level -= 1
        return node

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

    overrides: dict = {"visit_ImportFrom": _reroot}
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


def rewrite_pipeline(
    tree: ast.Module, *headers: ast.Module, copy: bool = True
) -> Result[str, ex.CDFError]:
    """Generates code from a python script ast with additional headers.

    Headers are prepended to the script and provide the ability to adjust the behavior of scripts
    without modifying the original source code.

    Args:
        tree: The ast of a python script.
        headers: Additional ast nodes to prepend to the tree in the order they are provided.
        copy: Whether to deepcopy the tree before rewriting. Defaults to True.

    Returns:
        str: The transformed code of the python script.
    """
    try:
        tree = deepcopy(tree) if copy else tree
        for header in reversed(headers):
            tree.body = [*header.body, *tree.body]
        tree.body = [*entrypoint_header.body, *tree.body]
        stringified_code = ast.unparse(
            ast.fix_missing_locations(rewriters[c.PIPELINES].visit(tree))
        )
        return Ok(
            "\n".join(line for line in stringified_code.splitlines() if line.strip())
        )
    except Exception as e:
        return Err(RewriteError(f"Failed to rewrite pipeline: {e}"))
