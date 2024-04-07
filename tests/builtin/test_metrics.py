from functools import reduce

import pytest

from cdf.builtin.metrics import (
    avg_value,
    count,
    max_value,
    median_value,
    min_value,
    mode_value,
    stdev_value,
    sum_value,
    unique,
    variance_value,
)


@pytest.fixture
def data():
    return [
        {"name": "Alice", "age": 25},
        {"name": "Bob", "age": 30},
        {"name": "Charlie", "age": 35},
        {"name": "David", "age": 40},
        {"name": "Eve", "age": 45},
        {"name": "Frank", "age": 50},
        {"name": "Alice", "age": 25},
    ]


def test_count(data):
    assert reduce(lambda metric, item: count(item, metric), data, 0) == 7


def test_unique(data):
    func = unique("name")
    assert reduce(lambda metric, item: func(item, metric), data, 0) == 6


def test_max_value(data):
    func = max_value("age")
    assert reduce(lambda metric, item: func(item, metric), data, 0) == 50


def test_min_value(data):
    func = min_value("age")
    assert reduce(lambda metric, item: func(item, metric), data, None) == 25


def test_sum_value(data):
    func = sum_value("age")
    assert reduce(lambda metric, item: func(item, metric), data, 0) == 250


def test_avg_value(data):
    func = avg_value("age")
    assert (
        reduce(lambda metric, item: func(item, metric), data, 0) == 35.714285714285715
    )


def test_median_value(data):
    func = median_value("age")
    assert reduce(lambda metric, item: func(item, metric), data, 0) == 35


def test_variance_value(data):
    func = variance_value("age")
    assert reduce(lambda metric, item: func(item, metric), data, 0) == 81.63265306122435


def test_stdev_value(data):
    func = stdev_value("age")
    assert reduce(lambda metric, item: func(item, metric), data, 0) == 9.035079029052504


def test_mode_value(data):
    func = mode_value("age")
    assert reduce(lambda metric, item: func(item, metric), data, 0) == 25
