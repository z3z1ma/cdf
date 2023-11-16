import os
import sys

from cdf import CDFSourceWrapper, cdf_resource, cdf_source


@cdf_resource
def osrandom(n: int = 100):
    for i in range(n):
        yield {"i": i, "payload": os.urandom(20).hex(sep=":"), "nested": {"a": 1 * i}}


@cdf_resource
def sys_vers(n: int = 50):
    for i in range(n):
        yield {"i": i, "payload": sys.version}


@cdf_source
def node_info():
    return osrandom(), sys_vers()


__CDF_SOURCE__ = dict(
    node_info=CDFSourceWrapper(
        factory=node_info,
        version=1,
        owners=("qa-team"),
        description="A source that emits random data.",
        tags=("random", "test"),
    )
)
