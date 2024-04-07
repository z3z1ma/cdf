import decimal
import typing as t
from pathlib import Path

import cdf.types.monads as M

PathLike = t.Union[str, Path]
Number = t.Union[int, float, decimal.Decimal]

__all__ = ["M", "PathLike", "Number"]
