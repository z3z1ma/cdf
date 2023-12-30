"""Built-in filters for CDF

They can be referenced via absolute import paths in a pipeline spec.

[[spec.pipelines]]
entrypoint = ".cities:us_zip_codes"
version = 2
filters.us_cities = [
  { entrypoint = "cdf.builtin.filters:not_empty", input.key = "latitude" },
  { entrypoint = "cdf.builtin.filters:not_empty", input.key = "longitude" },
]
"""


def not_empty(key: str):
    """Filters out items where a key is empty"""

    def _not_empty(item):
        return item[key] is not None and item[key] != ""

    return _not_empty


def not_null(key: str):
    """Filters out items where a key is null"""

    def _not_null(item):
        return item[key] is not None

    return _not_null


def gt(key: str, value: str):
    """Filters out items where a key is greater than a value"""

    def _greater_than(item):
        return item[key] > value

    return _greater_than


def lt(key: str, value: str):
    """Filters out items where a key is less than a value"""

    def _less_than(item):
        return item[key] < value

    return _less_than


def gte(key: str, value: str):
    """Filters out items where a key is greater than or equal to a value"""

    def _greater_than_or_equal(item):
        return item[key] >= value

    return _greater_than_or_equal


def lte(key: str, value: str):
    """Filters out items where a key is less than or equal to a value"""

    def _less_than_or_equal(item):
        return item[key] <= value

    return _less_than_or_equal


def eq(key: str, value: str):
    """Filters out items where a key is equal to a value"""

    def _equal(item):
        return item[key] == value

    return _equal


def ne(key: str, value: str):
    """Filters out items where a key is not equal to a value"""

    def _not_equal(item):
        return item[key] != value

    return _not_equal


def in_list(key: str, value: str):
    """Filters out items where a key is in a list of values"""

    def _in_list(item):
        return item[key] in value

    return _in_list


def not_in_list(key: str, value: str):
    """Filters out items where a key is not in a list of values"""

    def _not_in_list(item):
        return item[key] not in value

    return _not_in_list
