"""Built-in metrics for CDF

They can be referenced via absolute import paths in a pipeline spec.
"""

import bisect
import decimal
import math
import statistics
import typing as t
from collections import defaultdict

TNumber = t.TypeVar("TNumber", int, float, decimal.Decimal)

MetricFunc = t.Callable[[t.Any, TNumber], TNumber]


def count(_: t.Any, metric: TNumber = 0) -> TNumber:
    """Counts the number of items in a dataset"""
    return metric + 1


def unique(key: str) -> MetricFunc:
    """Counts the number of unique items in a dataset by a given key"""
    seen = set()

    def _unique(item: t.Any, _: t.Optional[TNumber] = None) -> int:
        k = item[key]
        if k not in seen:
            seen.add(k)
        return len(seen)

    return _unique


def max_value(key: str) -> MetricFunc:
    """Returns the maximum value of a key in a dataset"""

    def _max_value(item: t.Any, metric: t.Optional[TNumber] = None) -> TNumber:
        k = item[key]
        if metric is None:
            return k
        return max(metric, k)

    return _max_value


def min_value(key: str) -> MetricFunc:
    """Returns the minimum value of a key in a dataset"""

    def _min_value(item: t.Any, metric: t.Optional[TNumber] = None) -> TNumber:
        k = item[key]
        if metric is None:
            return k
        return min(metric, k)

    return _min_value


def sum_value(key: str) -> MetricFunc:
    """Returns the sum of a key in a dataset"""

    def _sum_value(item: t.Any, metric: TNumber = 0) -> TNumber:
        k = item[key]
        return metric + k

    return _sum_value


def avg_value(key: str) -> MetricFunc:
    """Returns the average of a key in a dataset"""
    n_sum, n_count = 0, 0

    def _avg_value(item: t.Any, _: t.Optional[TNumber] = None) -> TNumber:
        nonlocal n_sum, n_count
        k = item[key]
        n_sum += k
        n_count += 1
        return n_sum / n_count

    return _avg_value


def median_value(key: str, window: int = 1000) -> MetricFunc:
    """Returns the median of a key in a dataset"""
    arr = []

    def _median_value(item: t.Any, _: t.Optional[TNumber] = None) -> TNumber:
        nonlocal arr
        k = item[key]
        bisect.insort(arr, k)
        if len(arr) > window:
            del arr[0], arr[-1]
        return statistics.median(arr)

    return _median_value


def stdev_value(key: str) -> MetricFunc:
    """Returns the standard deviation of a key in a dataset"""
    n_sum, n_squared_sum, n_count = 0, 0, 0

    def _stdev_value(item: t.Any, _: t.Optional[TNumber] = None) -> float:
        nonlocal n_sum, n_squared_sum, n_count
        k = item[key]
        n_sum += k
        n_squared_sum += k**2
        n_count += 1
        mean = n_sum / n_count
        return math.sqrt(n_squared_sum / n_count - mean**2)

    return _stdev_value


def variance_value(key: str) -> MetricFunc:
    """Returns the variance of a key in a dataset"""
    n_sum, n_squared_sum, n_count = 0, 0, 0

    def _variance_value(item: t.Any, _: t.Optional[TNumber] = None) -> float:
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


def mode_value(key: str) -> MetricFunc:
    """Returns the mode of a key in a dataset."""
    frequency = defaultdict(int)

    def _mode_value(item: t.Any, _: t.Optional[TNumber] = None) -> TNumber:
        nonlocal frequency
        k = item[key]
        frequency[k] += 1
        return max(frequency.items(), key=lambda x: x[1])[0]

    return _mode_value
