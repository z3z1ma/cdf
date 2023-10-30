"""Contains classes & types common in CDF. This includes Monads."""
import typing as t
from dataclasses import dataclass
from pathlib import Path
from types import ModuleType

from cdf.core.source import ContinuousDataFlowSource
from cdf.core.utils import _augmented_path

T = t.TypeVar("T")
P = t.ParamSpec("P")


Loadable = t.Union[str, Path, ModuleType]

LazySource = t.Callable[[], ContinuousDataFlowSource]
SourceSpec = t.Dict[str, LazySource]


class Option(t.Generic[T]):
    def __init__(self, value: T | None) -> None:
        self._inner: t.Optional[T] = value

    def map(self, fn: t.Callable[[T], t.Optional[T]]) -> "Option":
        if self._inner is None:
            return self
        return Option(fn(self._inner))

    def flat_map(self, fn: t.Callable[[T], "Option"]) -> "Option":
        if self._inner is None:
            return self
        return fn(self._inner)

    def unwrap(self) -> T:
        if self._inner is None:
            raise ValueError("Cannot unwrap None")
        return self._inner

    def __call__(self, fn: t.Callable[[T], "Option"]) -> "Option":
        return self.flat_map(fn)

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


class PathAugmentedFn(t.Generic[P, T]):
    """A monad which wraps a callable and augments sys.path with a given path."""

    # Type Constructor
    def __init__(
        self,
        fn: t.Callable[P, T],
        paths: t.List[str] | str | None = None,
    ) -> None:
        """Unit of the monad."""
        paths = paths or []
        if not isinstance(paths, list):
            paths = [paths]

        def _fn_with_path(*args: P.args, **kwargs: P.kwargs) -> T:
            """Closure which augments sys.path and calls the callable."""
            with _augmented_path(*paths):
                return fn(*args, **kwargs)

        self.paths = paths
        self._fn = _fn_with_path

    # Bind (Combinator)
    def __call__(
        self, fn: t.Callable[[t.Callable[P, T]], "PathAugmentedFn"]
    ) -> "PathAugmentedFn":
        """Bind a callable to the monad. Only the last callable is bound."""
        return fn(self._fn)

    # Type Converter (For convenience)
    @property
    def value(self) -> t.Callable[P, T]:
        return self._fn
