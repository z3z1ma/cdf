import os
import sys

import dlt

from cdf import export_sources, source_spec


@dlt.resource
def osrandom(n: int = 100):
    for i in range(n):
        yield {"i": i, "payload": os.urandom(20).hex(sep=":"), "nested": {"a": 1 * i}}


@dlt.resource
def sys_vers(n: int = 50):
    for i in range(n):
        yield {"i": i, "payload": sys.version}


@dlt.source
def node_info():
    return osrandom(), sys_vers()


export_sources(
    node_info=source_spec(
        factory=node_info,
        version=1,
        owners=("qa-team"),
        description="A source that emits random data.",
        tags=("random", "test"),
    )
)
