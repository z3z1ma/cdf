"""Built-in filters for CDF

They can be referenced via absolute import paths in a pipeline spec.
"""

import typing as t

FilterFunc = t.Callable[[t.Any], bool]


def not_empty(key: str) -> FilterFunc:
    """Filters out items where a key is empty"""

    def _not_empty(item: t.Any) -> bool:
        if item.get(key) is None:
            return False
        if isinstance(item[key], str):
            return item[key].strip() != ""
        if isinstance(item[key], list):
            return len(item[key]) > 0
        if isinstance(item[key], dict):
            return len(item[key]) > 0
        return True

    return _not_empty


def not_null(key: str) -> FilterFunc:
    """Filters out items where a key is null"""

    def _not_null(item: t.Any) -> bool:
        return item.get(key) is not None

    return _not_null


def gt(key: str, value: t.Any) -> FilterFunc:
    """Filters out items where a key is greater than a value"""

    def _greater_than(item: t.Any) -> bool:
        return item[key] > value

    return _greater_than


def lt(key: str, value: t.Any) -> FilterFunc:
    """Filters out items where a key is less than a value"""

    def _less_than(item: t.Any) -> bool:
        return item[key] < value

    return _less_than


def gte(key: str, value: t.Any) -> FilterFunc:
    """Filters out items where a key is greater than or equal to a value"""

    def _greater_than_or_equal(item: t.Any) -> bool:
        return item[key] >= value

    return _greater_than_or_equal


def lte(key: str, value: t.Any) -> FilterFunc:
    """Filters out items where a key is less than or equal to a value"""

    def _less_than_or_equal(item: t.Any) -> bool:
        return item[key] <= value

    return _less_than_or_equal


def eq(key: str, value: t.Any) -> FilterFunc:
    """Filters out items where a key is equal to a value"""

    def _equal(item: t.Any) -> bool:
        return item[key] == value

    return _equal


def ne(key: str, value: t.Any) -> FilterFunc:
    """Filters out items where a key is not equal to a value"""

    def _not_equal(item: t.Any) -> bool:
        return item[key] != value

    return _not_equal


def in_list(key: str, value: t.List[str]) -> FilterFunc:
    """Filters out items where a key is in a list of values"""

    def _in_list(item: t.Any) -> bool:
        return item[key] in value

    return _in_list


def not_in_list(key: str, value: t.List[str]) -> FilterFunc:
    """Filters out items where a key is not in a list of values"""

    def _not_in_list(item: t.Any) -> bool:
        return item[key] not in value

    return _not_in_list
