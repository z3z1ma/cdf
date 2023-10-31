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


class EngineCredentials(t.NamedTuple):
    engine: str
    credentials: t.Dict[str, str] | str


DestinationSpec = t.Dict[str, EngineCredentials]


class Monad(t.Generic[A, Monoid]):
    def __init__(self, value: A, monoid: Monoid = None) -> None:
        self._value: A = value
        self._monoid: Monoid = monoid

    def map(self, fn: t.Callable[[A], B]) -> "Monad[B, Monoid]":
        # LOGIC HERE
        return Monad(fn(self._value), self._monoid)

    def flatmap(self, fn: t.Callable[[A], "Monad[B, Monoid]"]) -> "Monad[B, Monoid]":
        # LOGIC HERE
        return fn(self._value)

    def unwrap(self) -> A:
        return self._value

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self._value}, {self._monoid})"

    def __call__(self, fn: t.Callable[[A], B]) -> "Monad[B, Monoid]":
        return self.map(fn)

    def __str__(self) -> str:
        return self.__repr__()

    def __bool__(self) -> bool:
        return bool(self._value)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, self.__class__):
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


class Option(Monad[A, None]):
    def __init__(self, value: A = None) -> None:
        super().__init__(value)

    @property
    def option(self) -> A | None:
        return self._value

    def map(self, fn: t.Callable[[A], B | None]) -> "Option[B | None]":
        if self._value is None:
            return Option(None)
        return Option(fn(self._value))

    def flatmap(self, fn: t.Callable[[A], "Option[B | None]"]) -> "Option[B | None]":
        if self._value is None:
            return Option(None)
        return fn(self._value)

    def unwrap(self) -> A:
        if self._value is None:
            raise ValueError("Cannot unwrap None")
        return self._value

    def __bool__(self) -> bool:
        return self._value is not None

    def __iter__(self) -> t.Iterator[A]:
        if self._value is None:
            return
        yield self._value

    def __len__(self) -> int:
        return 1 if self._value is not None else 0


class Result(Monad[A | None, Exception | None]):
    def __init__(self, value: A | None, error: Exception | None = None) -> None:
        super().__init__(value, error)

    @property
    def error(self) -> Exception | None:
        return self._monoid

    @property
    def result(self) -> A | None:
        return self._value

    def map(self, fn: t.Callable[[A | None], B | None]) -> "Result[B]":
        if self.error is not None:
            return Result(None, self.error)
        try:
            return Result(fn(self.result), None)
        except Exception as e:
            return Result(None, e)

    def flatmap(self, fn: t.Callable[[A | None], "Result[B]"]) -> "Result[B]":
        if self.error is not None:
            return Result(None, self.error)
        return fn(self.result)

    def unwrap(self) -> A | None:
        if self.error is not None:
            raise self.error
        return self.result

    def expect(self) -> A:
        if self.error is not None:
            raise self.error
        if self.result is None:
            raise ValueError("Expected a result, got None")
        return self.result

    @classmethod
    def apply(
        cls, fn: t.Callable[P, A], *args: P.args, **kwargs: P.kwargs
    ) -> "Result[A]":
        return cls(None, None).map(lambda _: fn(*args, **kwargs))

    def __iter__(self) -> t.Iterator[t.Union[A, Exception, None]]:
        yield self.result
        yield self.error

    def __len__(self) -> int:
        return 1 if self.result is not None else 0
