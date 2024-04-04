import typing as t
from pathlib import Path

import cdf.types.monads as M

PathLike = t.Union[str, Path]

__all__ = ["M", "PathLike"]
