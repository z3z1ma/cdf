from __future__ import annotations

import datetime
import typing as t

from . import errors as injector_errors

PRIMITIVE_TYPES: t.Final[t.Tuple[t.Type, ...]] = (
    type(None),
    bool,
    int,
    float,
    str,
    datetime.date,
    datetime.time,
    datetime.datetime,
)


def check_type(
    value: t.Any, type_: type | None = None, desc: str | None = None
) -> None:
    """Check that value is of given type and raise error if not.

    Args:
        value: Value to check.
        type_: Type to check against.
        desc: Description for error.

    >>> import pytest; import cdf.injector
    >>> check_type("abc", str)
    >>> with pytest.raises(cdf.injector.InputConfigError):
    ...    check_type("abc", int)
    """
    if type_ is None:
        return

    if hasattr(type_, "__args__"):
        # TODO! Check nested typing types here.
        return
    else:
        types = (type_,)

    if not isinstance(value, types):
        raise injector_errors.InputConfigError(
            f"{desc} input mismatch types: {type(value)} is not {type_}"
        )


def nested_getattr(obj: t.Any, address: str) -> t.Any:
    """Return last attr of obj specified by "."-separated address.

    >>> nested_getattr([], "__class__.__name__")
    'list'
    """
    for address_part in address.split("."):
        obj = getattr(obj, address_part)
    return obj


def nested_contains(obj: t.Any, address: str) -> bool:
    """Check existence of last attr of obj specified by "."-separated address.

    >>> class QuickAttrDict(dict):
    ...     def __getattr__(self, key):
    ...         result = self[key]
    ...         return (
    ...             QuickAttrDict(result) if isinstance(result, dict)
    ...             else result
    ...         )
    >>> values = QuickAttrDict({"b": {"c": {"d": 1}}})
    >>> nested_contains(values, "b.c.d")
    True
    >>> nested_contains(values, "b.c")
    True
    >>> nested_contains(values, "b")
    True
    >>> nested_contains(values, "b.c.e")
    False
    >>> nested_contains(values, "b.e")
    False
    >>> nested_contains(values, "e")
    False
    """
    split_address = address.split(".")
    last_address_part = split_address[-1]
    for address_part in split_address[:-1]:
        if address_part in obj:
            obj = getattr(obj, address_part)
        else:
            return False
    return last_address_part in obj


def nested_setattr(obj: t.Any, address: str, value: t.Any) -> None:
    """Set last attr of obj specified by "."-separated address to given value.

    >>> import unittest.mock
    >>> a = unittest.mock.MagicMock()
    >>> nested_setattr(a, "b.c", 123)
    >>> a.b.c
    123
    """
    split_address = address.split(".")
    for idx, address_part in enumerate(split_address):
        if idx < len(split_address) - 1:
            obj = getattr(obj, address_part)
        else:
            setattr(obj, address_part, value)
