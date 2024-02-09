import functools
import types
import typing as t

import dlt
from dlt.common.pipeline import ExtractInfo, LoadInfo, NormalizeInfo
from dlt.extract.extract import data_to_sources
from dlt.pipeline.pipeline import Pipeline


class CDFReturn(Exception):
    """An exception designed to carry a value back to the caller."""

    def __init__(self, value: t.Any):
        self.value = value


@functools.wraps(Pipeline.extract)
def _extract_return_sources(
    self: Pipeline,
    data: t.Any,
    **kwargs: t.Any,
) -> None:
    with self._maybe_destination_capabilities():
        scoped_kwargs = kwargs.copy()
        scoped_kwargs.pop("workers", None)
        scoped_kwargs.pop("max_parallel_items", None)
        sources = data_to_sources(data, self, **scoped_kwargs)
    # Apply FFs?
    raise CDFReturn(sources)


@functools.wraps(Pipeline.extract)
def _extract(self: Pipeline, data: t.Any, **kwargs: t.Any) -> ExtractInfo:
    # Apply FFs?
    return self.extract(data, **kwargs)


@functools.wraps(Pipeline.normalize)
def _normalize(self: Pipeline, *args, **kwargs) -> NormalizeInfo:
    return self.normalize(*args, **kwargs)


@functools.wraps(Pipeline.load)
def _load(self: Pipeline, *args, **kwargs: t.Any) -> LoadInfo:
    return self.load(*args, **kwargs)


@functools.wraps(dlt.pipeline)
def return_source_pipeline(*args, **kwargs) -> Pipeline:
    pipe = dlt.pipeline(*args, **kwargs)
    setattr(pipe, "extract", types.MethodType(_extract_return_sources, pipe))
    return pipe


def data_pipeline(*args, **kwargs) -> Pipeline:
    """Injects cdf specific methods into the pipeline."""
    # TODO: Parametrize the destination argument
    pipe = dlt.pipeline(*args, **kwargs)
    setattr(pipe, "extract", types.MethodType(_extract, pipe))
    setattr(pipe, "normalize", types.MethodType(_normalize, pipe))
    setattr(pipe, "load", types.MethodType(_load, pipe))
    return pipe
