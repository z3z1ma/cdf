from cdf.core.source import resource as cdf_resource
from cdf.core.source import source as cdf_source
from cdf.core.source import to_cdf_meta


@cdf_resource
def gen():
    yield from range(10)


@cdf_source
def source1():
    return gen()


__CDF_SOURCE__ = to_cdf_meta(source1)
