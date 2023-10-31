"""Very simple pokemon pipeline, to be used as a starting point for new pipelines.

Available resources:
    fruits
    vegetables
"""
from pokemon import source

__CDF_SOURCE__ = dict(pokemon=source)
