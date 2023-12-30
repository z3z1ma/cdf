"""Built-in metrics for CDF

They can be referenced via absolute import paths in a pipeline spec.

[[spec.pipelines]]
entrypoint = ".cities:us_zip_codes"
version = 2
metrics.us_cities = [
  { entrypoint = "cdf.builtin.metrics:count" },
  { entrypoint = "cdf.builtin.metrics:mode", input.key = "city" },
]
"""
import bisect
import math
import statistics
from collections import defaultdict


def count(_, metric=0):
    """Counts the number of items in a dataset"""
    return metric + 1


def unique(key: str):
    """Counts the number of unique items in a dataset by a given key"""
    seen = set()

    def _unique(item, _=None):
        k = item[key]
        if k not in seen:
            seen.add(k)
        return len(seen)

    return _unique


def max_value(key: str):
    """Returns the maximum value of a key in a dataset"""

    def _max_value(item, metric=None):
        k = item[key]
        if metric is None:
            return k
        return max(metric, k)

    return _max_value


def min_value(key: str):
    """Returns the minimum value of a key in a dataset"""

    def _min_value(item, metric=None):
        k = item[key]
        if metric is None:
            return k
        return min(metric, k)

    return _min_value


def sum_value(key: str):
    """Returns the sum of a key in a dataset"""

    def _sum_value(item, metric=0):
        k = item[key]
        return metric + k

    return _sum_value


def avg_value(key: str):
    """Returns the average of a key in a dataset"""
    n_sum, n_count = 0, 0

    def _avg_value(item, _=None):
        nonlocal n_sum, n_count
        k = item[key]
        n_sum += k
        n_count += 1
        return n_sum / n_count

    return _avg_value


def median_value(key: str, window: int = 1000):
    """Returns the median of a key in a dataset"""
    arr = []

    def _median_value(item, _=None):
        nonlocal arr
        k = item[key]
        bisect.insort(arr, k)
        if len(arr) > window:
            del arr[0], arr[-1]
        return statistics.median(arr)

    return _median_value


def stdev_value(key: str):
    """Returns the standard deviation of a key in a dataset"""
    n_sum, n_squared_sum, n_count = 0, 0, 0

    def _stdev_value(item, _=None):
        nonlocal n_sum, n_squared_sum, n_count
        k = item[key]
        n_sum += k
        n_squared_sum += k**2
        n_count += 1
        mean = n_sum / n_count
        return math.sqrt(n_squared_sum / n_count - mean**2)

    return _stdev_value


def variance_value(key: str):
    """Returns the variance of a key in a dataset"""
    n_sum, n_squared_sum, n_count = 0, 0, 0

    def _variance_value(item, _=None):
        nonlocal n_sum, n_squared_sum, n_count
        k = item[key]
        n_sum += k
        n_squared_sum += k**2
        n_count += 1
        if n_count == 1:
            return 0
        mean = n_sum / n_count
        return (n_squared_sum / n_count) - mean**2

    return _variance_value


def mode_value(key: str):
    """Returns the mode of a key in a dataset."""
    frequency = defaultdict(int)

    def _mode_value(item, _=None):
        nonlocal frequency
        k = item[key]
        frequency[k] += 1
        return max(frequency.items(), key=lambda x: x[1])[0]

    return _mode_value
