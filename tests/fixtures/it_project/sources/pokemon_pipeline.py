"""Very simple pokemon pipeline, to be used as a starting point for new pipelines.

Available resources:
    fruits
    vegetables
"""
# this is injected by cdf through the workspace requirements.txt
import simple_salesforce as _  # noqa: F401 # type: ignore

# regular source
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
