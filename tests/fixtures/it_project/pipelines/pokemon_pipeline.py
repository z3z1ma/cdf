"""Very simple pokemon pipeline, to be used as a starting point for new pipelines.

Available resources:
    fruits
    vegetables
"""
import typing as t

# this is injected by cdf through the workspace requirements.txt
import simple_salesforce

# this is only available in the top level dev environment, workspace environment
# should be overlayed on top of it
import sklearn

# regular source
from pipelines.pokemon import source

from cdf import pipeline_spec

if t.TYPE_CHECKING:
    from cdf import PipeGen


def get_some_pokemon() -> "PipeGen":
    poke = source()
    pipeline = yield poke
    return pipeline.run(poke)


__CDF_PIPELINES__ = [
    pipeline_spec(
        name="pokemon",
        pipe=get_some_pokemon,
        version=1,
        owners=("qa-team"),
        description="Extracts pokemon data from an API.",
        tags=("live", "simple", "test"),
    )
]
