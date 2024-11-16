"""Contains monadic types and functions for working with them."""

from __future__ import annotations

import abc
import asyncio
import functools
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
E = t.TypeVar("E", bound=BaseException, covariant=True)  # The type of the error inside the Result
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

        def __rshift__(self, func: t.Callable[[T], "Result[U, E]"]) -> "Result[U, E]": ...

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
    def __init__(self, coro: t.Coroutine[t.Any, t.Any, T]) -> None:
        """Initializes a Promise with a coroutine.

        Args:
            coro: An asyncio coroutine that will produce a result of type T.
        """
        if not asyncio.iscoroutine(coro):
            raise ValueError("The provided argument must be an asyncio coroutine.")
        self._task = asyncio.ensure_future(coro)

    def __await__(self) -> t.Generator[t.Any, None, T]:
        """Allows the Promise to be awaited."""
        return self._task.__await__()

    def __hash__(self) -> int:
        """Returns the hash of the underlying task."""
        return hash(self._task)

    @classmethod
    def pure(cls, value: T) -> "Promise[T]":
        """Creates a Promise that is already resolved with a value.

        Args:
            value: The value to resolve the Promise with.

        Returns:
            A new Promise that is already resolved with the value.
        """
        return cls.resolve(value)

    @classmethod
    def resolve(cls, value: "T | Promise[T]") -> "Promise[T]":
        """Resolves a value or another Promise into a Promise.

        Args:
            value: The value or Promise to resolve.

        Returns:
            A Promise that wraps the provided value or the result of the provided Promise.
        """
        if isinstance(value, Promise):
            return value
        elif asyncio.isfuture(value) or asyncio.iscoroutine(value):
            return cls(value)
        else:

            async def coro():
                return value

            return cls(coro())

    @classmethod
    def reject(cls, exception: BaseException) -> "Promise[T]":
        """Creates a Promise that is already rejected with an exception.

        Args:
            exception: The exception to reject the Promise with.

        Returns:
            A Promise that will raise the provided exception when awaited.
        """

        async def coro():
            raise exception

        return cls(coro())

    @classmethod
    def from_value(cls, value: T) -> "Promise[T]":
        """Alias for resolve, creates a Promise that is already resolved with a value."""
        return cls.resolve(value)

    @classmethod
    def from_exception(cls, exception: BaseException) -> "Promise[T]":
        """Alias for reject, creates a Promise that is already rejected with an exception."""
        return cls.reject(exception)

    def set_result(self, result: T) -> None:
        """Sets a result on the Promise.

        Args:
            result: The result to set on the Promise.
        """
        if not self._task.done():
            self._task.set_result(result)

    def set_exception(self, exception: BaseException) -> None:
        """Sets an exception on the Promise.

        Args:
            exception: The exception to set on the Promise.
        """
        if not self._task.done():
            self._task.set_exception(exception)

    def then(self, on_fulfilled: t.Callable[[T], "U | Promise[U]"]) -> "Promise[U]":
        """Chains a function to be executed when the Promise is fulfilled.

        Args:
            on_fulfilled: A function that takes the result and returns a new value or Promise.

        Returns:
            A new Promise resulting from the application of the provided function.
        """

        async def coro():
            try:
                result = await self._task
                value = on_fulfilled(result)
                if isinstance(value, Promise):
                    return await value
                elif asyncio.iscoroutine(value) or asyncio.isfuture(value):
                    return await value
                else:
                    return value
            except Exception as e:
                raise e

        return Promise(coro())

    def catch(self, on_rejected: t.Callable[[Exception], "U | Promise[U]"]) -> "Promise[U]":
        """Adds a rejection handler to the Promise.

        Args:
            on_rejected: A function that handles exceptions and returns a new value or Promise.

        Returns:
            A new Promise that has the result of the rejection handler if an exception occurs.
        """

        async def coro():
            try:
                return await self._task
            except Exception as e:
                value = on_rejected(e)
                if isinstance(value, Promise):
                    return await value
                elif asyncio.iscoroutine(value) or asyncio.isfuture(value):
                    return await value
                else:
                    return value

        return Promise(coro())

    def bind(self, func: t.Callable[[T], "Promise[U]"]) -> "Promise[U]":
        """Applies a function that returns a Promise to the result of this Promise.

        Args:
            func: A function that takes the result and returns a new Promise.

        Returns:
            A new Promise resulting from the application of the provided function.
        """

        def on_fulfilled(result: T) -> "Promise[U]":
            return func(result)

        return self.then(on_fulfilled)

    def map(self, func: t.Callable[[T], U]) -> "Promise[U]":
        """Applies a synchronous function to the result of the Promise.

        Args:
            func: A function that takes the result and returns a new value.

        Returns:
            A new Promise containing the result of the function.
        """

        async def coro():
            result = await self._task
            return func(result)

        return Promise(coro())

    def filter(self, predicate: t.Callable[[T], bool]) -> "Promise[T]":
        """Filters the result of the Promise based on a predicate.

        Args:
            predicate: A function that returns True if the value should be kept.

        Returns:
            A new Promise containing the result if the predicate is True, else raises ValueError.
        """

        async def coro():
            result = await self._task
            if predicate(result):
                return result
            else:
                raise ValueError("Filter predicate failed")

        return Promise(coro())

    def unwrap(self) -> T:
        """Synchronously retrieves the result of the Promise.

        Returns:
            The result of the Promise.

        Raises:
            Exception: If the Promise is not yet completed or if an exception occurred.
        """
        return self.get_result()

    def unwrap_or(self, default: T) -> T:
        """Retrieves the result or returns a default value if an exception occurred.

        Args:
            default: The default value to return if the Promise is rejected.

        Returns:
            The result of the Promise or the default value.
        """
        try:
            return self.get_result()
        except Exception:
            return default

    @classmethod
    def lift(cls, func: t.Callable[[U], T]) -> t.Callable[["U | Promise[U]"], "Promise[T]"]:
        """Lifts a synchronous function to operate on Promises.

        Args:
            func: A synchronous function to lift.

        Returns:
            A function that takes a value or Promise and returns a Promise.
        """

        def wrapper(value: "U | Promise[U]") -> "Promise[T]":
            promise_value = cls.resolve(value)
            return promise_value.map(func)

        return wrapper

    @classmethod
    def all(cls, promises: t.Iterable["Promise[T]"]) -> "Promise[t.List[T]]":
        """Waits for all Promises to be fulfilled.

        Args:
            promises: An iterable of Promises.

        Returns:
            A Promise that resolves to a list of results.
        """

        async def coro():
            return await asyncio.gather(*(p._task for p in promises))

        return Promise(coro())

    @classmethod
    def race(cls, promises: t.Iterable["Promise[T]"]) -> "Promise[T]":
        """Returns a Promise that resolves or rejects as soon as one of the Promises does.

        Args:
            promises: An iterable of Promises.

        Returns:
            A Promise that resolves or rejects with the outcome of the first settled Promise.
        """

        async def coro():
            tasks = [p._task for p in promises]
            done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            result_task = done.pop()
            return await result_task

        return Promise(coro())

    def get_result(self) -> T:
        """Retrieves the result of the Promise synchronously.

        Returns:
            The result of the Promise.

        Raises:
            Exception: If the Promise is not yet completed or if an exception occurred.
        """
        if self._task.done():
            return self._task.result()
        else:
            raise Exception("Promise is not yet completed.")

    def is_fulfilled(self) -> bool:
        """Checks if the Promise has been fulfilled.

        Returns:
            True if the Promise is fulfilled, False otherwise.
        """
        return self._task.done() and not self._task.cancelled() and self._task.exception() is None

    def is_rejected(self) -> bool:
        """Checks if the Promise has been rejected.

        Returns:
            True if the Promise is rejected, False otherwise.
        """
        return self._task.done() and self._task.exception() is not None

    def is_pending(self) -> bool:
        """Checks if the Promise is still pending.

        Returns:
            True if the Promise is pending, False otherwise.
        """
        return not self._task.done()


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
        raise NotImplementedError("State cannot directly return a value without an initial state.")

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
    def lift(cls, func: t.Callable[[U], A]) -> t.Callable[["U | State[S, U]"], "State[S, A]"]:
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
