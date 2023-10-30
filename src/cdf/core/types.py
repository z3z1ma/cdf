"""Contains classes & types common in CDF. This includes Monads."""
import typing as t
from pathlib import Path
from types import ModuleType

from cdf.core.source import ContinuousDataFlowSource

T = t.TypeVar("T")
P = t.ParamSpec("P")

A = t.TypeVar("A")
B = t.TypeVar("B")

Monoid = t.TypeVar("Monoid")

Loadable = t.Union[str, Path, ModuleType]

LazySource = t.Callable[[], ContinuousDataFlowSource]
SourceSpec = t.Dict[str, LazySource]


class Monad(t.Generic[A, Monoid]):
    def __init__(self, value: A, monoid: Monoid) -> None:
        self._value: A = value
        self._monoid: Monoid = monoid

    def map(self, fn: t.Callable[[A], B]) -> "Monad[B, Monoid]":
        return Monad(fn(self._value), self._monoid)

    def flatmap(self, fn: t.Callable[[A], "Monad[B, Monoid]"]) -> "Monad[B, Monoid]":
        return fn(self._value)

    def unwrap(self) -> A:
        return self._value

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self._value}, {self._monoid})"

    __call__ = map
    __str__ = __repr__

    def __bool__(self) -> bool:
        return bool(self._value)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Monad):
            return False
        return self._value == other._value and self._monoid == other._monoid

    def __ne__(self, other: object) -> bool:
        return not self.__eq__(other)

    def __hash__(self) -> int:
        return hash((self._value, self._monoid))

    def __iter__(self) -> t.Iterator[A]:
        yield self._value

    def __len__(self) -> int:
        return 1


class Option(t.Generic[A]):
    def __init__(self, value: A | None = None) -> None:
        self._inner: A | None = value

    def map(self, fn: t.Callable[[A], B]) -> "Option[B]":
        if self._inner is None:
            return Option(None)
        return Option(fn(self._inner))

    def flatmap(self, fn: t.Callable[[A], "Option[B]"]) -> "Option[B]":
        if self._inner is None:
            return Option(None)
        return fn(self._inner)

    def unwrap(self) -> A:
        if self._inner is None:
            raise ValueError("Cannot unwrap None")
        return self._inner

    def __repr__(self) -> str:
        return f"Option({self._inner})"

    __call__ = map
    __str__ = __repr__

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

    def __iter__(self) -> t.Iterator[A]:
        if self._inner is None:
            return
        yield self._inner

    def __len__(self) -> int:
        return 1 if self._inner is not None else 0


class Result(t.Generic[A]):
    def __init__(self, value: A | None, error: Exception | None = None) -> None:
        self._inner: A | None = value
        self._error: Exception | None = error

    def map(self, fn: t.Callable[[A | None], B | None]) -> "Result[B]":
        if self._error is not None:
            return Result(None, self._error)
        try:
            return Result(fn(self._inner), None)
        except Exception as e:
            return Result(None, e)

    def flatmap(self, fn: t.Callable[[A | None], "Result[B]"]) -> "Result[B]":
        if self._error is not None:
            return Result(None, self._error)
        return fn(self._inner)

    def unwrap(self) -> A | None:
        if self._error is not None:
            raise self._error
        return self._inner

    @classmethod
    def apply(
        cls, fn: t.Callable[P, A], *args: P.args, **kwargs: P.kwargs
    ) -> "Result[A]":
        try:
            return Result(fn(*args, **kwargs), None)
        except Exception as e:
            return Result(None, e)

    def __repr__(self) -> str:
        return f"Result({self._inner}, {self._error})"

    __call__ = map
    __str__ = __repr__

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

    def __iter__(self) -> t.Iterator[t.Union[A, Exception, None]]:
        yield self._inner
        yield self._error

    def __len__(self) -> int:
        return 1 if self._inner is not None else 0
