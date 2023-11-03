"""Very simple pokemon pipeline, to be used as a starting point for new pipelines.

Available resources:
    fruits
    vegetables
"""
from pokemon import source

from cdf import CDFSourceMeta

__CDF_SOURCE__ = dict(
    pokemon=CDFSourceMeta(
        deferred_fn=source,
        version=1,
        owners=("qa-team"),
        description="Extracts pokemon data from an API.",
        tags=("live", "simple", "test"),
    )
)
