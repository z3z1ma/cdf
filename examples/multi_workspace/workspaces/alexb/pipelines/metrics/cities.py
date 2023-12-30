"""
Metrics for the cities dataset.

Many metrics can be represented via cdf.builtin.metrics, but sometimes you need
to write your own. This is an example of how to do that. These metrics are collected
and stored in the metrics table. They are attached in the pipeline spec.
"""


def longest_name(item, metric=0):
    return max(metric, len(item["city"]))


def min_latitude(item, metric=None):
    if metric is None:
        return item["latitude"]
    return min(metric, item["latitude"] or metric)


def max_latitude(item, metric=None):
    if metric is None:
        return item["latitude"]
    return max(metric, item["latitude"] or metric)


def min_longitude(item, metric=None):
    if metric is None:
        return item["longitude"]
    return min(metric, item["longitude"] or metric)


def max_longitude(item, metric=None):
    if metric is None:
        return item["longitude"]
    return max(metric, item["longitude"] or metric)
