from cdf.builtin.filters import (
    eq,
    gt,
    gte,
    in_list,
    lt,
    lte,
    ne,
    not_empty,
    not_in_list,
    not_null,
)


def test_eq():
    assert eq("name", "Alice")({"name": "Alice"})


def test_ne():
    assert ne("name", "Alice")({"name": "Bob"})


def test_gt():
    assert gt("age", 30)({"age": 35})


def test_gte():
    assert gte("age", 30)({"age": 30})


def test_lt():
    assert lt("age", 30)({"age": 25})


def test_lte():
    assert lte("age", 30)({"age": 30})


def test_in_list():
    assert in_list("name", ["Alice", "Bob"])({"name": "Alice"})


def test_not_in_list():
    assert not_in_list("name", ["Alice", "Bob"])({"name": "Charlie"})


def test_not_empty():
    assert not_empty("name")({"name": "Alice"})
    assert not_empty("name")({"name": 0})
    assert not_empty("name")({"name": False})

    assert not_empty("name")({"name": ""}) is False
    assert not_empty("name")({"name": []}) is False
    assert not_empty("name")({"name": {}}) is False
    assert not_empty("name")({"name": None}) is False


def test_not_null():
    assert not_null("name")({"name": "Alice"})
    assert not_null("name")({"name": 0})
    assert not_null("name")({"name": False})
    assert not_null("name")({"name": []})
    assert not_null("name")({"name": {}})

    assert not_null("name")({"name": None}) is False
    assert not_null("name")({"whatever": 1}) is False
