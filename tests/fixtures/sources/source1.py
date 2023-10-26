from cdf.core.source import resource as cdf_resource
from cdf.core.source import source as cdf_source


@cdf_resource
def gen():
    yield from range(10)


@cdf_source
def source1():
    return gen()


def setup() -> None:
    source1()
