from cdf.core.source import resource as cdf_resource
from cdf.core.source import source as cdf_source


@cdf_resource
def gen():
    yield from range(10)


@cdf_source
def source1():
    return gen()


__CDF_SOURCE__ = {"source1": source1}
