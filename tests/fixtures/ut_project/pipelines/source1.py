import dlt


@dlt.resource
def gen():
    yield from range(10)


@dlt.source
def source1():
    return gen()


__CDF_PIPELINES__ = [
    {
        "pipe": source1,
        "version": 1,
        "owners": ("qa-team"),
        "description": "A source that enumerates integers.",
        "tags": ("deterministic", "test"),
    }
]
