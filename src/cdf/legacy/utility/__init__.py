import typing as t
from operator import itemgetter

TDict = t.TypeVar("TDict", bound=t.Dict[str, t.Any])


def find_item(
    lst: t.List[TDict], key: t.Union[t.Callable[[TDict], t.Any], str], value: t.Any
) -> TDict:
    """Find an item in a list by a key-value pair.

    Example:
        >>> find_item([{"name": "Alice"}, {"name": "Bob"}], "name", "Bob")
        {"name": "Bob"}

    Args:
        lst: The list to search.
        key: The key function to extract the value from an item or the key name.
        value: The value to find.

    Returns:
        The item with the matching value.
    """
    fn = itemgetter(key) if isinstance(key, str) else key
    return next((item for item in lst if fn(item) == value))
