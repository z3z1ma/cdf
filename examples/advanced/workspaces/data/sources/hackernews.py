import dlt
from hn import HN

from cdf.core.source import CDFSourceMeta

hn = HN()


@dlt.resource(name="stories")
def stories(limit: int):
    for story in hn.get_stories(story_type="newest", limit=limit):
        yield dict(story)


@dlt.source(name="hackernews")
def hackernews(limit: int = 10):
    return (stories(limit=limit),)


__CDF_SOURCE__ = dict(
    hackernews=CDFSourceMeta(
        deferred_fn=hackernews,
        version=1,
        owners=("qa-team"),
        description="Extracts hackernews data from an API.",
        tags=("live", "simple", "test"),
    )
)
