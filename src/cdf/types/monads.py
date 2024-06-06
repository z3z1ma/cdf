"""Contains monadic types and functions for working with them."""

from __future__ import annotations

import abc
import asyncio
import functools
import inspect
import sys
import typing as t

from typing_extensions import Self

if sys.version_info < (3, 10):
    from typing_extensions import ParamSpec
else:
    from typing import ParamSpec

T = t.TypeVar("T")  # The type of the value inside the Monad
U = t.TypeVar("U")  # The transformed type of the value inside the Monad
K = t.TypeVar("K")  # A known type that is not necessarily the same as T
L = t.TypeVar("L")  # A known type that is not necessarily the same as U
E = t.TypeVar(
    "E", bound=BaseException, covariant=True
)  # The type of the error inside the Result
P = ParamSpec("P")

TState = t.TypeVar("TState")  # The type of the state
TMonad = t.TypeVar("TMonad", bound="Monad")  # Generic Self type for Monad


class Monad(t.Generic[T], abc.ABC):
    def __init__(self, value: T) -> None:
        self._value = value

    def __hash__(self) -> int:
        return hash(self._value)

    @abc.abstractmethod
    def bind(self, func: t.Callable[[T], "Monad[U]"]) -> "Monad[U]":
        pass

    @abc.abstractmethod
    def map(self, func: t.Callable[[T], U]) -> "Monad[U]":
        pass

    @abc.abstractmethod
    def filter(self, predicate: t.Callable[[T], bool]) -> Self:
        pass

    @abc.abstractmethod
    def unwrap(self) -> T:
        pass

    @abc.abstractmethod
    def unwrap_or(self, default: U) -> t.Union[T, U]:
        pass

    def __call__(self, func: t.Callable[[T], "Monad[U]"]) -> "Monad[U]":
        return self.bind(func)

    def __rshift__(self, func: t.Callable[[T], "Monad[U]"]) -> "Monad[U]":
        return self.bind(func)


class Maybe(Monad[T], abc.ABC):
    @classmethod
    def pure(cls, value: K) -> "Maybe[K]":
        """Creates a Maybe with a value."""
        return Just(value)

    @abc.abstractmethod
    def is_just(self) -> bool:
        pass

    @abc.abstractmethod
    def is_nothing(self) -> bool:
        pass

    if t.TYPE_CHECKING:

        def bind(self, func: t.Callable[[T], "Maybe[U]"]) -> "Maybe[U]": ...

        def map(self, func: t.Callable[[T], U]) -> "Maybe[U]": ...

        def filter(self, predicate: t.Callable[[T], bool]) -> "Maybe[T]": ...

    def unwrap(self) -> T:
        """Unwraps the value of the Maybe.

        Returns:
            The unwrapped value.
        """
        if self.is_just():
            return self._value
        else:
            raise ValueError("Cannot unwrap Nothing.")

    def unwrap_or(self, default: U) -> t.Union[T, U]:
        """Tries to unwrap the Maybe, returning a default value if the Maybe is Nothing.

        Args:
            default: The value to return if unwrapping Nothing.

        Returns:
            The unwrapped value or the default value.
        """
        if self.is_just():
            return self._value
        else:
            return default

    @classmethod
    def lift(cls, func: t.Callable[[U], K]) -> t.Callable[["U | Maybe[U]"], "Maybe[K]"]:
        """Lifts a function to work within the Maybe monad.

        Args:
            func: A function to lift.

        Returns:
            A new function that returns a Maybe value.
        """

        @functools.wraps(func)
        def wrapper(value: U | Maybe[U]) -> Maybe[K]:
            if isinstance(value, Maybe):
                return value.map(func)  # type: ignore
            value = t.cast(U, value)
            try:
                result = func(value)
                if result is None:
                    return Nothing()
                return Just(result)
            except Exception:
                return Nothing()

        return wrapper

    def __iter__(self) -> t.Iterator[T]:
        """Allows safely unwrapping the value of the Maybe using a for construct."""
        if self.is_just():
            yield self.unwrap()


class Just(Maybe[T]):
    def bind(self, func: t.Callable[[T], Maybe[U]]) -> Maybe[U]:
        """Applies a function to the value inside the Just.

        Args:
            func: A function that takes a value of type T and returns a Maybe containing a value of type U.

        Returns:
            The result of applying the function to the value inside the Just.
        """
        return func(self._value)

    def map(self, func: t.Callable[[T], U]) -> "Maybe[U]":
        """Applies a mapping function to the value inside the Just.

        Args:
            func: A function that takes a value of type T and returns a value of type U.

        Returns:
            A new Just containing the result of applying the function to the value inside the Just.
        """
        try:
            result = func(self._value)
            if result is None:
                return Nothing()
            return Just(result)
        except Exception:
            return Nothing()

    def filter(self, predicate: t.Callable[[T], bool]) -> Maybe[T]:
        """Filters the value inside the Just based on a predicate.

        Args:
            predicate: A function that takes a value of type T and returns a boolean.

        Returns:
            A new Just containing the value inside the Just if the predicate holds.
        """
        if predicate(self._value):
            return self
        else:
            return Nothing()

    def is_just(self) -> bool:
        """Returns True if the Maybe is a Just."""
        return True

    def is_nothing(self) -> bool:
        """Returns False if the Maybe is a Just."""
        return False

    def __repr__(self) -> str:
        return f"Just({self._value})"


class Nothing(Maybe[T]):
    def __init__(self) -> None:
        super().__init__(t.cast(T, None))

    def bind(self, func: t.Callable[[T], Maybe[U]]) -> "Nothing[T]":
        """Applies a function to the value inside the Just.

        Args:
            func: A function that takes a value of type T and returns a Maybe containing a value of type U.

        Returns:
            The result of applying the function to the value inside the Just.
        """
        return self

    def map(self, func: t.Callable[[T], U]) -> "Nothing[T]":
        """Applies a mapping function to the value inside the Just.

        Args:
            func: A function that takes a value of type T and returns a value of type U.

        Returns:
            A new Just containing the result of applying the function to the value inside the Just.
        """
        return self

    def filter(self, predicate: t.Callable[[T], bool]) -> "Nothing[T]":
        """Filters the value inside the Just based on a predicate.

        Args:
            predicate: A function that takes a value of type T and returns a boolean.

        Returns:
            A new Just containing the value inside the Just if the predicate holds.
        """
        return self

    def is_just(self) -> bool:
        """Returns False if the Maybe is a Nothing."""
        return False

    def is_nothing(self) -> bool:
        """Returns True if the Maybe is a Nothing."""
        return True

    def __repr__(self) -> str:
        return "Nothing()"


class Result(Monad[T], t.Generic[T, E]):
    @classmethod
    def pure(cls, value: K) -> "Result[K, E]":
        """Creates an Ok with a value."""
        return Ok(value)

    @abc.abstractmethod
    def is_ok(self) -> bool:
        pass

    @abc.abstractmethod
    def is_err(self) -> bool:
        pass

    @abc.abstractmethod
    def unwrap(self) -> T:
        pass

    @abc.abstractmethod
    def unwrap_or(self, default: U) -> t.Union[T, U]:
        pass

    @abc.abstractmethod
    def unwrap_err(self) -> BaseException:
        pass

    @abc.abstractmethod
    def to_parts(self) -> t.Tuple[T, E | None]:
        pass

    @classmethod
    def lift(
        cls, func: t.Callable[[U], K]
    ) -> t.Callable[["U | Result[U, Exception]"], "Result[K, Exception]"]:
        """Transforms a function to work with arguments and output wrapped in Result monads.

        Args:
            func: A function that takes any number of arguments and returns a value of type T.

        Returns:
            A function that takes the same number of unwrapped arguments and returns a Result-wrapped result.
        """

        def wrapper(result: U | Result[U, Exception]) -> Result[K, Exception]:
            if isinstance(result, Result):
                return result.map(func)
            result = t.cast(U, result)
            try:
                return Ok(func(result))
            except Exception as e:
                return Err(e)

        if hasattr(func, "__defaults__") and func.__defaults__:
            default = func.__defaults__[0]
            wrapper.__defaults__ = (default,)

        return wrapper

    if t.TYPE_CHECKING:

        def bind(self, func: t.Callable[[T], "Result[U, E]"]) -> "Result[U, E]": ...

        def map(self, func: t.Callable[[T], U]) -> "Result[U, E]": ...

        def filter(self, predicate: t.Callable[[T], bool]) -> "Result[T, E]": ...

        def __call__(self, func: t.Callable[[T], "Result[U, E]"]) -> "Result[U, E]": ...

        def __rshift__(
            self, func: t.Callable[[T], "Result[U, E]"]
        ) -> "Result[U, E]": ...

    def __iter__(self) -> t.Iterator[T]:
        """Allows safely unwrapping the value of the Result using a for construct."""
        if self.is_ok():
            yield self.unwrap()


class Ok(Result[T, E]):
    def bind(self, func: t.Callable[[T], Result[U, E]]) -> Result[U, E]:
        """Applies a function to the result of the Ok.

        Args:
            func: A function that takes a value of type T and returns a Result containing a value of type U.

        Returns:
            A new Result containing the result of the original Result after applying the function.
        """
        return func(self._value)

    def map(self, func: t.Callable[[T], U]) -> Result[U, E]:
        """Applies a mapping function to the result of the Ok.

        Args:
            func: A function that takes a value of type T and returns a value of type U.

        Returns:
            A new Ok containing the result of the original Ok after applying the function.
        """
        try:
            return Ok(func(self._value))
        except Exception as e:
            return Err(t.cast(E, e))

    def is_ok(self) -> bool:
        """Returns True if the Result is an Ok."""
        return True

    def is_err(self) -> bool:
        """Returns False if the Result is an Ok."""
        return False

    def unwrap(self) -> T:
        """Unwraps the value of the Ok.

        Returns:
            The unwrapped value.
        """
        return self._value

    def unwrap_or(self, default: t.Any) -> T:
        """Tries to unwrap the Ok, returning a default value if unwrapping raises an exception.

        Args:
            default: The value to return if unwrapping raises an exception.

        Returns:
            The unwrapped value or the default value if an exception is raised.
        """
        return self._value

    def unwrap_err(self) -> BaseException:
        """Raises a ValueError since the Result is an Ok."""
        raise ValueError("Called unwrap_err on Ok")

    def filter(self, predicate: t.Callable[[T], bool]) -> Result[T, E]:
        """Filters the result of the Ok based on a predicate.

        Args:
            predicate: A function that takes a value of type T and returns a boolean.
            error: The error to return if the predicate does not hold.

        Returns:
            A new Result containing the result of the original Result if the predicate holds.
        """
        if predicate(self._value):
            return self
        else:
            return Err(t.cast(E, ValueError("Predicate does not hold")))

    def to_parts(self) -> t.Tuple[T, None]:
        """Unpacks the value of the Ok."""
        return (self._value, None)

    def __repr__(self) -> str:
        return f"Ok({self._value})"


class Err(Result[T, E]):
    def __init__(self, error: E) -> None:
        """Initializes an Err with an error.

        Args:
            error: The error to wrap in the Err.
        """
        self._error = error

    def __hash__(self) -> int:
        return hash(self._error)

    def bind(self, func: t.Callable[[T], Result[U, E]]) -> "Err[T, E]":
        """Applies a function to the result of the Err.

        Args:
            func: A function that takes a value of type T and returns a Result containing a value of type U.

        Returns:
            An Err containing the original error.
        """
        return self

    def map(self, func: t.Callable[[T], U]) -> "Err[T, E]":
        """Applies a mapping function to the result of the Err.

        Args:
            func: A function that takes a value of type T and returns a value of type U.

        Returns:
            An Err containing the original error.
        """
        return self

    def is_ok(self) -> bool:
        """Returns False if the Result is an Err."""
        return False

    def is_err(self) -> bool:
        """Returns True if the Result is an Err."""
        return True

    def unwrap(self) -> T:
        """Raises a ValueError since the Result is an Err."""
        raise self._error

    def unwrap_or(self, default: U) -> U:
        """Returns a default value since the Result is an Err.

        Args:
            default: The value to return.

        Returns:
            The default value.
        """
        return default

    def unwrap_err(self) -> BaseException:
        """Unwraps the error of the Err.

        Returns:
            The unwrapped error.
        """
        return self._error

    def filter(self, predicate: t.Callable[[T], bool]) -> "Err[T, E]":
        """Filters the result of the Err based on a predicate.

        Args:
            predicate: A function that takes a value of type T and returns a boolean.

        Returns:
            An Err containing the original error.
        """
        return self

    def to_parts(self) -> t.Tuple[None, E]:
        """Unpacks the error of the Err."""
        return (None, self._error)

    def __repr__(self) -> str:
        return f"Err({self._error})"


class Promise(t.Generic[T], t.Awaitable[T], Monad[T]):
    def __init__(
        self,
        coro_func: t.Callable[P, t.Coroutine[None, None, T]],
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> None:
        """Initializes a Promise with a coroutine function.

        Args:
            coro_func: A coroutine function that returns a value of type T.
            args: Positional arguments to pass to the coroutine function.
            kwargs: Keyword arguments to pass to the coroutine function.
        """
        self._loop = asyncio.get_event_loop()
        if callable(coro_func):
            coro = coro_func(*args, **kwargs)
        elif inspect.iscoroutine(coro_func):
            coro = t.cast(t.Coroutine[None, None, T], coro_func)
        else:
            raise ValueError("Invalid coroutine function")
        self._future: asyncio.Future[T] = asyncio.ensure_future(coro, loop=self._loop)

    @classmethod
    def pure(cls, value: K) -> "Promise[K]":
        """Creates a Promise that is already resolved with a value.

        Args:
            value: The value to resolve the Promise with.

        Returns:
            A new Promise that is already resolved with the value.
        """
        return cls.from_value(value)

    def __hash__(self) -> int:
        return hash(self._future)

    def __await__(self):
        """Allows the Promise to be awaited."""
        yield from self._future.__await__()
        return (yield from self._future.__await__())

    def set_result(self, result: T) -> None:
        """Sets a result on the Promise.

        Args:
            result: The result to set on the Promise.
        """
        if not self._future.done():
            self._loop.call_soon_threadsafe(self._future.set_result, result)

    def set_exception(self, exception: Exception) -> None:
        """Sets an exception on the Promise.

        Args:
            exception: The exception to set on the Promise.
        """
        if not self._future.done():
            self._loop.call_soon_threadsafe(self._future.set_exception, exception)

    def bind(self, func: t.Callable[[T], "Promise[U]"]) -> "Promise[U]":
        """Applies a function to the result of the Promise.

        Args:
            func: A function that takes a value of type T and returns a Promise containing a value of type U.

        Returns:
            A new Promise containing the result of the original Promise after applying the function.
        """

        async def bound_coro() -> U:
            try:
                value = await self
                next_promise = func(value)
                return await next_promise
            except Exception as e:
                future = self._loop.create_future()
                future.set_exception(e)
                return t.cast(U, await future)

        return Promise(bound_coro)

    def map(self, func: t.Callable[[T], U]) -> "Promise[U]":
        """Applies a mapping function to the result of the Promise.

        Args:
            func: A function that takes a value of type T and returns a value of type U.

        Returns:
            A new Promise containing the result of the original Promise after applying the function.
        """

        async def mapped_coro() -> U:
            try:
                value = await self
                return func(value)
            except Exception as e:
                future = self._loop.create_future()
                future.set_exception(e)
                return t.cast(U, await future)

        return Promise(mapped_coro)

    then = map  # syntactic sugar, equivalent to map

    def filter(self, predicate: t.Callable[[T], bool]) -> "Promise[T]":
        """Filters the result of the Promise based on a predicate.

        Args:
            predicate: A function that takes a value of type T and returns a boolean.

        Returns:
            A new Promise containing the result of the original Promise if the predicate holds.
        """

        async def filtered_coro() -> T:
            try:
                value = await self
                if predicate(value):
                    return value
                else:
                    raise ValueError("Filter predicate failed")
            except Exception as e:
                future = self._loop.create_future()
                future.set_exception(e)
                return await future

        return Promise(filtered_coro)

    def unwrap(self) -> T:
        return self._loop.run_until_complete(self)

    def unwrap_or(self, default: T) -> T:
        """Tries to unwrap the Promise, returning a default value if unwrapping raises an exception.

        Args:
            default: The value to return if unwrapping raises an exception.

        Returns:
            The unwrapped value or the default value if an exception is raised.
        """
        try:
            return self._loop.run_until_complete(self)
        except Exception:
            return default

    @classmethod
    def from_value(cls, value: T) -> "Promise[T]":
        """Creates a Promise that is already resolved with a value.

        Args:
            value: The value to resolve the Promise with.

        Returns:
            A new Promise that is already resolved with the value.
        """

        async def _fut():
            return value

        return cls(_fut)

    @classmethod
    def from_exception(cls, exception: BaseException) -> "Promise[T]":
        """Creates a Promise that is already resolved with an exception.

        Args:
            exception: The exception to resolve the Promise with.

        Returns:
            A new Promise that is already resolved with the exception.
        """

        async def _fut():
            raise exception

        return cls(_fut)

    @classmethod
    def lift(
        cls, func: t.Callable[[U], T]
    ) -> t.Callable[["U | Promise[U]"], "Promise[T]"]:
        """
        Lifts a synchronous function to work within the Promise context,
        making it return a Promise of the result and allowing it to be used
        with Promise inputs.

        Args:
            func: A synchronous function that returns a value of type T.

        Returns:
            A function that, when called, returns a Promise wrapping the result of the original function.
        """

        @functools.wraps(func)
        def wrapper(value: "U | Promise[U]") -> "Promise[T]":
            if isinstance(value, Promise):
                return value.map(func)
            value = t.cast(U, value)

            async def async_wrapper() -> T:
                return func(value)

            return cls(async_wrapper)

        return wrapper


class Lazy(Monad[T]):
    def __init__(self, computation: t.Callable[[], T]) -> None:
        """Initializes a Lazy monad with a computation that will be executed lazily.

        Args:
            computation: A function that takes no arguments and returns a value of type T.
        """
        self._computation = computation
        self._value = None
        self._evaluated = False

    @classmethod
    def pure(cls, value: T) -> "Lazy[T]":
        """Creates a Lazy monad with a pure value."""
        return cls(lambda: value)

    def evaluate(self) -> T:
        """Evaluates the computation if it has not been evaluated yet and caches the result.

        Returns:
            The result of the computation.
        """
        if not self._evaluated:
            self._value = self._computation()
            self._evaluated = True
        return t.cast(T, self._value)

    def bind(self, func: t.Callable[[T], "Lazy[U]"]) -> "Lazy[U]":
        """Lazily applies a function to the result of the current computation.

        Args:
            func: A function that takes a value of type T and returns a Lazy monad containing a value of type U.

        Returns:
            A new Lazy monad containing the result of the computation after applying the function.
        """
        return Lazy(lambda: func(self.evaluate()).evaluate())

    def map(self, func: t.Callable[[T], U]) -> "Lazy[U]":
        """Lazily applies a mapping function to the result of the computation.

        Args:
            func: A function that takes a value of type T and returns a value of type U.

        Returns:
            A new Lazy monad containing the result of the computation after applying the function.
        """
        return Lazy(lambda: func(self.evaluate()))

    def filter(self, predicate: t.Callable[[T], bool]) -> "Lazy[T]":
        """Lazily filters the result of the computation based on a predicate.

        Args:
            predicate: A function that takes a value of type T and returns a boolean.

        Returns:
            A new Lazy monad containing the result of the computation if the predicate holds.
        """

        def filter_computation():
            result = self.evaluate()
            if predicate(result):
                return result
            else:
                raise ValueError("Predicate does not hold for the value.")

        return Lazy(filter_computation)

    def unwrap(self) -> T:
        """Forces evaluation of the computation and returns its result.

        Returns:
            The result of the computation.
        """
        return self.evaluate()

    def unwrap_or(self, default: T) -> T:
        """Tries to evaluate the computation, returning a default value if evaluation raises an exception.

        Args:
            default: The value to return if the computation raises an exception.

        Returns:
            The result of the computation or the default value if an exception is raised.
        """
        try:
            return self.evaluate()
        except Exception:
            return default

    @classmethod
    def lift(cls, func: t.Callable[[U], T]) -> t.Callable[["U | Lazy[U]"], "Lazy[T]"]:
        """Transforms a function to work with arguments and output wrapped in Lazy monads.

        Args:
            func: A function that takes any number of arguments and returns a value of type U.

        Returns:
            A function that takes the same number of Lazy-wrapped arguments and returns a Lazy-wrapped result.
        """

        @functools.wraps(func)
        def wrapper(value: "U | Lazy[U]") -> "Lazy[T]":
            if isinstance(value, Lazy):
                return value.map(func)
            value = t.cast(U, value)

            def computation() -> T:
                return func(value)

            return cls(computation)

        return wrapper


Defer = Lazy  # Defer is an alias for Lazy

S = t.TypeVar("S")  # State type
A = t.TypeVar("A")  # Return type
B = t.TypeVar("B")  # Transformed type


class State(t.Generic[S, A], Monad[A], abc.ABC):
    def __init__(self, run_state: t.Callable[[S], t.Tuple[A, S]]) -> None:
        self.run_state = run_state

    def bind(self, func: t.Callable[[A], "State[S, B]"]) -> "State[S, B]":
        def new_run_state(s: S) -> t.Tuple[B, S]:
            a, state_prime = self.run_state(s)
            return func(a).run_state(state_prime)

        return State(new_run_state)

    def map(self, func: t.Callable[[A], B]) -> "State[S, B]":
        def new_run_state(s: S) -> t.Tuple[B, S]:
            a, state_prime = self.run_state(s)
            return func(a), state_prime

        return State(new_run_state)

    def filter(self, predicate: t.Callable[[A], bool]) -> "State[S, A]":
        def new_run_state(s: S) -> t.Tuple[A, S]:
            a, state_prime = self.run_state(s)
            if predicate(a):
                return a, state_prime
            else:
                raise ValueError("Value does not satisfy predicate")

        return State(new_run_state)

    def unwrap(self) -> A:
        raise NotImplementedError(
            "State cannot be directly unwrapped without providing an initial state."
        )

    def unwrap_or(self, default: B) -> t.Union[A, B]:
        raise NotImplementedError(
            "State cannot directly return a value without an initial state."
        )

    def __hash__(self) -> int:
        return id(self.run_state)

    @staticmethod
    def pure(value: A) -> "State[S, A]":
        return State(lambda s: (value, s))

    def __call__(self, state: S) -> t.Tuple[A, S]:
        return self.run_state(state)

    def __repr__(self) -> str:
        return f"State({self.run_state})"

    @classmethod
    def lift(
        cls, func: t.Callable[[U], A]
    ) -> t.Callable[["U | State[S, U]"], "State[S, A]"]:
        """Lifts a function to work within the State monad.
        Args:
            func: A function to lift.
        Returns:
            A new function that returns a State value.
        """

        @functools.wraps(func)
        def wrapper(value: "U | State[S, U]") -> "State[S, A]":
            if isinstance(value, State):
                return value.map(func)
            value = t.cast(U, value)

            def run_state(s: S) -> t.Tuple[A, S]:
                return func(value), s

            return cls(run_state)

        return wrapper


# Aliases for monadic converters
# to_<monad> is the pure function
# <monad> is the lift function

to_maybe = just = Maybe.pure
nothing = Nothing[t.Any]()
maybe = Maybe.lift

to_result = ok = Result.pure
error = lambda e: Err(e)  # noqa: E731
result = Result.lift

to_promise = Promise.pure
promise = Promise.lift

to_lazy = Lazy.pure
lazy = Lazy.lift

to_deferred = Defer.pure
deferred = Defer.lift

to_state = State.pure
state = State.lift

# to_io = IO.pure
# io = IO.lift
