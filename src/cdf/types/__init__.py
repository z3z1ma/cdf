"""A module for shared types."""

import sys
import typing as t
from pathlib import Path

import cdf.types.monads as M

if t.TYPE_CHECKING:
    import decimal

PathLike = t.Union[str, Path]
Number = t.Union[int, float, "decimal.Decimal"]

if sys.version_info < (3, 10):
    from typing_extensions import ParamSpec
else:
    from typing import ParamSpec

P = ParamSpec("P")

__all__ = ["M", "P", "PathLike", "Number"]
