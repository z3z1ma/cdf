"""Contains classes & types common in CDF. This includes Monads."""
import typing as t
from pathlib import Path
from types import ModuleType

from cdf.core.source import ContinuousDataFlowSource

T = t.TypeVar("T")
S = t.TypeVar("S")
P = t.ParamSpec("P")


Loadable = t.Union[str, Path, ModuleType]

LazySource = t.Callable[[], ContinuousDataFlowSource]
SourceSpec = t.Dict[str, LazySource]


class Option(t.Generic[T]):
    def __init__(self, value: T | None) -> None:
        self._inner: T | None = value

    def map(self, fn: t.Callable[[T], S]) -> "Option[S]":
        if self._inner is None:
            return Option(None)
        return Option(fn(self._inner))

    def flatmap(self, fn: t.Callable[[T], "Option[S]"]) -> "Option[S]":
        if self._inner is None:
            return Option(None)
        return fn(self._inner)

    def unwrap(self) -> T:
        if self._inner is None:
            raise ValueError("Cannot unwrap None")
        return self._inner

    def __call__(self, fn: t.Callable[[T], "Option[S]"]) -> "Option[S]":
        return self.flatmap(fn)

    def __repr__(self) -> str:
        return f"Option({self._inner})"

    def __str__(self) -> str:
        return f"Option({self._inner})"

    def __bool__(self) -> bool:
        return self._inner is not None

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Option):
            return False
        return self._inner == other._inner

    def __ne__(self, other: object) -> bool:
        return not self.__eq__(other)

    def __hash__(self) -> int:
        return hash(self._inner)

    def __iter__(self) -> t.Iterator[T]:
        if self._inner is None:
            return
        yield self._inner

    def __len__(self) -> int:
        return 1 if self._inner is not None else 0


class Result(t.Generic[T]):
    def __init__(self, value: T | None, error: Exception | None) -> None:
        self._inner: T | None = value
        self._error: Exception | None = error

    def map(self, fn: t.Callable[[T | None], S | None]) -> "Result[S]":
        if self._error is not None:
            return Result(None, self._error)
        try:
            return Result(fn(self._inner), None)
        except Exception as e:
            return Result(None, e)

    def flatmap(self, fn: t.Callable[[T | None], "Result[S]"]) -> "Result[S]":
        if self._error is not None:
            return Result(None, self._error)
        return fn(self._inner)

    def unwrap(self) -> T | None:
        if self._error is not None:
            raise self._error
        return self._inner

    def __call__(self, fn: t.Callable[[T | None], "Result[S]"]) -> "Result[S]":
        return self.flatmap(fn)

    def __repr__(self) -> str:
        return f"Result({self._inner}, {self._error})"

    def __str__(self) -> str:
        return f"Result({self._inner}, {self._error})"

    def __bool__(self) -> bool:
        return self._inner is not None

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Result):
            return False
        return self._inner == other._inner and self._error == other._error

    def __ne__(self, other: object) -> bool:
        return not self.__eq__(other)

    def __hash__(self) -> int:
        return hash((self._inner, self._error))

    def __iter__(self) -> t.Iterator[t.Union[T, Exception, None]]:
        yield self._inner
        yield self._error

    def __len__(self) -> int:
        return 1 if self._inner is not None else 0
