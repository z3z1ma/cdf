"""This module provides an external hook mechanism for dlt pipelines. 

It is actuated by the rewriter module which replaces the dlt.pipeline constructor with a custom constructor. This
custom constructor forwards the call to dlt.pipeline and then injects the hooks into the pipeline instance. The hooks
are responsible for capturing sources from the extract method and rerouting them to a container. The container is a
module-level variable inserted by runpy.runpath. This mechanism allows the cdf to access the sources from a pipeline
script which is otherwise a black box. It affords maximum flexibility to the user in terms of how they want to
author their pipeline scripts whilst still enabling cdf to perform its functions.
"""
import functools
import sys
import types
import typing as t

import dlt
from dlt.common.pipeline import ExtractInfo, LoadInfo, NormalizeInfo
from dlt.extract.extract import data_to_sources
from dlt.pipeline.pipeline import Pipeline

import cdf.core.constants as c


@functools.wraps(Pipeline.extract)
def _extract_capture_sources(
    self: Pipeline,
    data: t.Any,
    **kwargs: t.Any,
) -> t.List[dlt.sources.DltSource]:
    """Intercept runtime sources from the extract method."""
    with self._maybe_destination_capabilities():
        scoped_kwargs = kwargs.copy()
        scoped_kwargs.pop("workers", None)
        scoped_kwargs.pop("max_parallel_items", None)
        sources = data_to_sources(data, self, **scoped_kwargs)
    container = sys.modules["__main__"].__dict__[c.SOURCE_CONTAINER]
    for source in sources:
        container.add(source)
    return []


@functools.wraps(Pipeline.extract)
def _extract(self: Pipeline, data: t.Any, **kwargs: t.Any) -> ExtractInfo:
    """Extract data from a source."""
    return self.extract(data, **kwargs)


@functools.wraps(Pipeline.normalize)
def _normalize(self: Pipeline, *args, **kwargs) -> NormalizeInfo:
    """Normalize extracted data."""
    return self.normalize(*args, **kwargs)


@functools.wraps(Pipeline.load)
def _load(self: Pipeline, *args, **kwargs: t.Any) -> LoadInfo:
    """Load normalized data."""
    return self.load(*args, **kwargs)


@functools.wraps(dlt.pipeline)
def intercepting_pipeline(*args, **kwargs) -> Pipeline:
    """A pipeline that captures sources from the extract method and reroutes them to a container."""
    pipe = dlt.pipeline(*args, **kwargs)
    setattr(pipe, "extract", types.MethodType(_extract_capture_sources, pipe))
    return pipe


@functools.wraps(dlt.pipeline)
def pipeline(*args, **kwargs) -> Pipeline:
    """A standard pipeline with injected hooks."""
    # TODO: Parametrize the destination argument
    pipe = dlt.pipeline(*args, **kwargs)
    setattr(pipe, "extract", types.MethodType(_extract, pipe))
    setattr(pipe, "normalize", types.MethodType(_normalize, pipe))
    setattr(pipe, "load", types.MethodType(_load, pipe))
    return pipe
