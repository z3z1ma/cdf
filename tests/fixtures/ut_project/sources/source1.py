from cdf import CDFSourceWrapper, cdf_resource, cdf_source


@cdf_resource
def gen():
    yield from range(10)


@cdf_source
def source1():
    return gen()


__CDF_SOURCE__ = dict(
    source1=CDFSourceWrapper(
        factory=source1,
        version=1,
        owners=("qa-team"),
        description="A source that enumerates integers.",
        tags=("deterministic", "test"),
    )
)
