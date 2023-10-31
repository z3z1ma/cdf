import os
import sys

from cdf.core.source import resource as cdf_resource
from cdf.core.source import source as cdf_source
from cdf.core.source import to_cdf_meta


@cdf_resource
def osrandom(n: int = 10):
    for i in range(n):
        yield {"i": i, "payload": os.urandom(10).hex(sep=":")}


@cdf_resource
def sys_vers():
    for i in range(10):
        yield {"i": i, "payload": sys.version}


@cdf_source
def node_info():
    return osrandom(10), sys_vers()


__CDF_SOURCE__ = to_cdf_meta(node_info)
