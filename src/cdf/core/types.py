"""Contains classes & types common in CDF. This includes Monads."""
import typing as t
from pathlib import Path
from types import ModuleType

from cdf.core.source import ContinuousDataFlowSource
from cdf.core.utils import _augmented_path

T = t.TypeVar("T")
P = t.ParamSpec("P")


Loadable = t.Union[str, Path, ModuleType]

LazySource = t.Callable[[], ContinuousDataFlowSource]
SourceSpec = t.Dict[str, LazySource]


class MaybeFn(t.Generic[P, T]):
    """A monad which wraps a callable and returns None if an exception is raised."""

    # Type Constructor
    def __init__(self, fn: t.Callable[P, T]) -> None:
        """Unit of the monad."""

        def maybe(*args: P.args, **kwargs: P.kwargs) -> t.Optional[T]:
            try:
                return fn(*args, **kwargs)
            except Exception:
                return None

        self._fn = maybe

    # Bind (Combinator)
    def __call__(
        self, fn: t.Callable[[t.Callable[P, t.Optional[T]]], "MaybeFn"]
    ) -> "MaybeFn":
        """Bind a callable to the monad."""
        return fn(self._fn)

    # Type Converter (For convenience)
    def value(self, *args: P.args, **kwargs: P.kwargs) -> t.Optional[T]:
        """Return the value of the monad."""
        return self._fn(*args, **kwargs)


T = t.TypeVar("T")
P = t.ParamSpec("P")


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
