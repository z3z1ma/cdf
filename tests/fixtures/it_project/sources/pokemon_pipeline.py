"""Very simple pokemon pipeline, to be used as a starting point for new pipelines.

Available resources:
    fruits
    vegetables
"""
# this is injected by cdf through the workspace requirements.txt
import simple_salesforce

# this is only available in the top level dev environment, workspace environment
# should be overlayed on top of it
import sklearn

# regular source
from sources.pokemon import source

from cdf import source_spec

__CDF_SOURCE__ = {
    "pokemon": source_spec(
        factory=source,
        version=1,
        owners=("qa-team"),
        description="Extracts pokemon data from an API.",
        tags=("live", "simple", "test"),
    )
}
