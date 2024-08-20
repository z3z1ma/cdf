<a id="__init__"></a>

# \_\_init\_\_

<a id="types"></a>

# types

A module for shared types.

<a id="types.monads"></a>

# types.monads

Contains monadic types and functions for working with them.

<a id="types.monads.T"></a>

#### T

The type of the value inside the Monad

<a id="types.monads.U"></a>

#### U

The transformed type of the value inside the Monad

<a id="types.monads.K"></a>

#### K

A known type that is not necessarily the same as T

<a id="types.monads.L"></a>

#### L

A known type that is not necessarily the same as U

<a id="types.monads.E"></a>

#### E

The type of the error inside the Result

<a id="types.monads.TState"></a>

#### TState

The type of the state

<a id="types.monads.TMonad"></a>

#### TMonad

Generic Self type for Monad

<a id="types.monads.Maybe"></a>

## Maybe Objects

```python
class Maybe(Monad[T], abc.ABC)
```

<a id="types.monads.Maybe.pure"></a>

#### pure

```python
@classmethod
def pure(cls, value: K) -> "Maybe[K]"
```

Creates a Maybe with a value.

<a id="types.monads.Maybe.unwrap"></a>

#### unwrap

```python
def unwrap() -> T
```

Unwraps the value of the Maybe.

**Returns**:

  The unwrapped value.

<a id="types.monads.Maybe.unwrap_or"></a>

#### unwrap\_or

```python
def unwrap_or(default: U) -> t.Union[T, U]
```

Tries to unwrap the Maybe, returning a default value if the Maybe is Nothing.

**Arguments**:

- `default` - The value to return if unwrapping Nothing.
  

**Returns**:

  The unwrapped value or the default value.

<a id="types.monads.Maybe.lift"></a>

#### lift

```python
@classmethod
def lift(cls, func: t.Callable[[U],
                               K]) -> t.Callable[["U | Maybe[U]"], "Maybe[K]"]
```

Lifts a function to work within the Maybe monad.

**Arguments**:

- `func` - A function to lift.
  

**Returns**:

  A new function that returns a Maybe value.

<a id="types.monads.Maybe.__iter__"></a>

#### \_\_iter\_\_

```python
def __iter__() -> t.Iterator[T]
```

Allows safely unwrapping the value of the Maybe using a for construct.

<a id="types.monads.Just"></a>

## Just Objects

```python
class Just(Maybe[T])
```

<a id="types.monads.Just.bind"></a>

#### bind

```python
def bind(func: t.Callable[[T], Maybe[U]]) -> Maybe[U]
```

Applies a function to the value inside the Just.

**Arguments**:

- `func` - A function that takes a value of type T and returns a Maybe containing a value of type U.
  

**Returns**:

  The result of applying the function to the value inside the Just.

<a id="types.monads.Just.map"></a>

#### map

```python
def map(func: t.Callable[[T], U]) -> "Maybe[U]"
```

Applies a mapping function to the value inside the Just.

**Arguments**:

- `func` - A function that takes a value of type T and returns a value of type U.
  

**Returns**:

  A new Just containing the result of applying the function to the value inside the Just.

<a id="types.monads.Just.filter"></a>

#### filter

```python
def filter(predicate: t.Callable[[T], bool]) -> Maybe[T]
```

Filters the value inside the Just based on a predicate.

**Arguments**:

- `predicate` - A function that takes a value of type T and returns a boolean.
  

**Returns**:

  A new Just containing the value inside the Just if the predicate holds.

<a id="types.monads.Just.is_just"></a>

#### is\_just

```python
def is_just() -> bool
```

Returns True if the Maybe is a Just.

<a id="types.monads.Just.is_nothing"></a>

#### is\_nothing

```python
def is_nothing() -> bool
```

Returns False if the Maybe is a Just.

<a id="types.monads.Nothing"></a>

## Nothing Objects

```python
class Nothing(Maybe[T])
```

<a id="types.monads.Nothing.bind"></a>

#### bind

```python
def bind(func: t.Callable[[T], Maybe[U]]) -> "Nothing[T]"
```

Applies a function to the value inside the Just.

**Arguments**:

- `func` - A function that takes a value of type T and returns a Maybe containing a value of type U.
  

**Returns**:

  The result of applying the function to the value inside the Just.

<a id="types.monads.Nothing.map"></a>

#### map

```python
def map(func: t.Callable[[T], U]) -> "Nothing[T]"
```

Applies a mapping function to the value inside the Just.

**Arguments**:

- `func` - A function that takes a value of type T and returns a value of type U.
  

**Returns**:

  A new Just containing the result of applying the function to the value inside the Just.

<a id="types.monads.Nothing.filter"></a>

#### filter

```python
def filter(predicate: t.Callable[[T], bool]) -> "Nothing[T]"
```

Filters the value inside the Just based on a predicate.

**Arguments**:

- `predicate` - A function that takes a value of type T and returns a boolean.
  

**Returns**:

  A new Just containing the value inside the Just if the predicate holds.

<a id="types.monads.Nothing.is_just"></a>

#### is\_just

```python
def is_just() -> bool
```

Returns False if the Maybe is a Nothing.

<a id="types.monads.Nothing.is_nothing"></a>

#### is\_nothing

```python
def is_nothing() -> bool
```

Returns True if the Maybe is a Nothing.

<a id="types.monads.Result"></a>

## Result Objects

```python
class Result(Monad[T], t.Generic[T, E])
```

<a id="types.monads.Result.pure"></a>

#### pure

```python
@classmethod
def pure(cls, value: K) -> "Result[K, E]"
```

Creates an Ok with a value.

<a id="types.monads.Result.lift"></a>

#### lift

```python
@classmethod
def lift(
    cls, func: t.Callable[[U], K]
) -> t.Callable[["U | Result[U, Exception]"], "Result[K, Exception]"]
```

Transforms a function to work with arguments and output wrapped in Result monads.

**Arguments**:

- `func` - A function that takes any number of arguments and returns a value of type T.
  

**Returns**:

  A function that takes the same number of unwrapped arguments and returns a Result-wrapped result.

<a id="types.monads.Result.__iter__"></a>

#### \_\_iter\_\_

```python
def __iter__() -> t.Iterator[T]
```

Allows safely unwrapping the value of the Result using a for construct.

<a id="types.monads.Ok"></a>

## Ok Objects

```python
class Ok(Result[T, E])
```

<a id="types.monads.Ok.bind"></a>

#### bind

```python
def bind(func: t.Callable[[T], Result[U, E]]) -> Result[U, E]
```

Applies a function to the result of the Ok.

**Arguments**:

- `func` - A function that takes a value of type T and returns a Result containing a value of type U.
  

**Returns**:

  A new Result containing the result of the original Result after applying the function.

<a id="types.monads.Ok.map"></a>

#### map

```python
def map(func: t.Callable[[T], U]) -> Result[U, E]
```

Applies a mapping function to the result of the Ok.

**Arguments**:

- `func` - A function that takes a value of type T and returns a value of type U.
  

**Returns**:

  A new Ok containing the result of the original Ok after applying the function.

<a id="types.monads.Ok.is_ok"></a>

#### is\_ok

```python
def is_ok() -> bool
```

Returns True if the Result is an Ok.

<a id="types.monads.Ok.is_err"></a>

#### is\_err

```python
def is_err() -> bool
```

Returns False if the Result is an Ok.

<a id="types.monads.Ok.unwrap"></a>

#### unwrap

```python
def unwrap() -> T
```

Unwraps the value of the Ok.

**Returns**:

  The unwrapped value.

<a id="types.monads.Ok.unwrap_or"></a>

#### unwrap\_or

```python
def unwrap_or(default: t.Any) -> T
```

Tries to unwrap the Ok, returning a default value if unwrapping raises an exception.

**Arguments**:

- `default` - The value to return if unwrapping raises an exception.
  

**Returns**:

  The unwrapped value or the default value if an exception is raised.

<a id="types.monads.Ok.unwrap_err"></a>

#### unwrap\_err

```python
def unwrap_err() -> BaseException
```

Raises a ValueError since the Result is an Ok.

<a id="types.monads.Ok.filter"></a>

#### filter

```python
def filter(predicate: t.Callable[[T], bool]) -> Result[T, E]
```

Filters the result of the Ok based on a predicate.

**Arguments**:

- `predicate` - A function that takes a value of type T and returns a boolean.
- `error` - The error to return if the predicate does not hold.
  

**Returns**:

  A new Result containing the result of the original Result if the predicate holds.

<a id="types.monads.Ok.to_parts"></a>

#### to\_parts

```python
def to_parts() -> t.Tuple[T, None]
```

Unpacks the value of the Ok.

<a id="types.monads.Err"></a>

## Err Objects

```python
class Err(Result[T, E])
```

<a id="types.monads.Err.__init__"></a>

#### \_\_init\_\_

```python
def __init__(error: E) -> None
```

Initializes an Err with an error.

**Arguments**:

- `error` - The error to wrap in the Err.

<a id="types.monads.Err.bind"></a>

#### bind

```python
def bind(func: t.Callable[[T], Result[U, E]]) -> "Err[T, E]"
```

Applies a function to the result of the Err.

**Arguments**:

- `func` - A function that takes a value of type T and returns a Result containing a value of type U.
  

**Returns**:

  An Err containing the original error.

<a id="types.monads.Err.map"></a>

#### map

```python
def map(func: t.Callable[[T], U]) -> "Err[T, E]"
```

Applies a mapping function to the result of the Err.

**Arguments**:

- `func` - A function that takes a value of type T and returns a value of type U.
  

**Returns**:

  An Err containing the original error.

<a id="types.monads.Err.is_ok"></a>

#### is\_ok

```python
def is_ok() -> bool
```

Returns False if the Result is an Err.

<a id="types.monads.Err.is_err"></a>

#### is\_err

```python
def is_err() -> bool
```

Returns True if the Result is an Err.

<a id="types.monads.Err.unwrap"></a>

#### unwrap

```python
def unwrap() -> T
```

Raises a ValueError since the Result is an Err.

<a id="types.monads.Err.unwrap_or"></a>

#### unwrap\_or

```python
def unwrap_or(default: U) -> U
```

Returns a default value since the Result is an Err.

**Arguments**:

- `default` - The value to return.
  

**Returns**:

  The default value.

<a id="types.monads.Err.unwrap_err"></a>

#### unwrap\_err

```python
def unwrap_err() -> BaseException
```

Unwraps the error of the Err.

**Returns**:

  The unwrapped error.

<a id="types.monads.Err.filter"></a>

#### filter

```python
def filter(predicate: t.Callable[[T], bool]) -> "Err[T, E]"
```

Filters the result of the Err based on a predicate.

**Arguments**:

- `predicate` - A function that takes a value of type T and returns a boolean.
  

**Returns**:

  An Err containing the original error.

<a id="types.monads.Err.to_parts"></a>

#### to\_parts

```python
def to_parts() -> t.Tuple[None, E]
```

Unpacks the error of the Err.

<a id="types.monads.Promise"></a>

## Promise Objects

```python
class Promise(t.Generic[T], t.Awaitable[T], Monad[T])
```

<a id="types.monads.Promise.__init__"></a>

#### \_\_init\_\_

```python
def __init__(coro_func: t.Callable[P, t.Coroutine[None, None, T]], *args:
             P.args, **kwargs: P.kwargs) -> None
```

Initializes a Promise with a coroutine function.

**Arguments**:

- `coro_func` - A coroutine function that returns a value of type T.
- `args` - Positional arguments to pass to the coroutine function.
- `kwargs` - Keyword arguments to pass to the coroutine function.

<a id="types.monads.Promise.pure"></a>

#### pure

```python
@classmethod
def pure(cls, value: K) -> "Promise[K]"
```

Creates a Promise that is already resolved with a value.

**Arguments**:

- `value` - The value to resolve the Promise with.
  

**Returns**:

  A new Promise that is already resolved with the value.

<a id="types.monads.Promise.__await__"></a>

#### \_\_await\_\_

```python
def __await__()
```

Allows the Promise to be awaited.

<a id="types.monads.Promise.set_result"></a>

#### set\_result

```python
def set_result(result: T) -> None
```

Sets a result on the Promise.

**Arguments**:

- `result` - The result to set on the Promise.

<a id="types.monads.Promise.set_exception"></a>

#### set\_exception

```python
def set_exception(exception: Exception) -> None
```

Sets an exception on the Promise.

**Arguments**:

- `exception` - The exception to set on the Promise.

<a id="types.monads.Promise.bind"></a>

#### bind

```python
def bind(func: t.Callable[[T], "Promise[U]"]) -> "Promise[U]"
```

Applies a function to the result of the Promise.

**Arguments**:

- `func` - A function that takes a value of type T and returns a Promise containing a value of type U.
  

**Returns**:

  A new Promise containing the result of the original Promise after applying the function.

<a id="types.monads.Promise.map"></a>

#### map

```python
def map(func: t.Callable[[T], U]) -> "Promise[U]"
```

Applies a mapping function to the result of the Promise.

**Arguments**:

- `func` - A function that takes a value of type T and returns a value of type U.
  

**Returns**:

  A new Promise containing the result of the original Promise after applying the function.

<a id="types.monads.Promise.then"></a>

#### then

syntactic sugar, equivalent to map

<a id="types.monads.Promise.filter"></a>

#### filter

```python
def filter(predicate: t.Callable[[T], bool]) -> "Promise[T]"
```

Filters the result of the Promise based on a predicate.

**Arguments**:

- `predicate` - A function that takes a value of type T and returns a boolean.
  

**Returns**:

  A new Promise containing the result of the original Promise if the predicate holds.

<a id="types.monads.Promise.unwrap_or"></a>

#### unwrap\_or

```python
def unwrap_or(default: T) -> T
```

Tries to unwrap the Promise, returning a default value if unwrapping raises an exception.

**Arguments**:

- `default` - The value to return if unwrapping raises an exception.
  

**Returns**:

  The unwrapped value or the default value if an exception is raised.

<a id="types.monads.Promise.from_value"></a>

#### from\_value

```python
@classmethod
def from_value(cls, value: T) -> "Promise[T]"
```

Creates a Promise that is already resolved with a value.

**Arguments**:

- `value` - The value to resolve the Promise with.
  

**Returns**:

  A new Promise that is already resolved with the value.

<a id="types.monads.Promise.from_exception"></a>

#### from\_exception

```python
@classmethod
def from_exception(cls, exception: BaseException) -> "Promise[T]"
```

Creates a Promise that is already resolved with an exception.

**Arguments**:

- `exception` - The exception to resolve the Promise with.
  

**Returns**:

  A new Promise that is already resolved with the exception.

<a id="types.monads.Promise.lift"></a>

#### lift

```python
@classmethod
def lift(
        cls,
        func: t.Callable[[U],
                         T]) -> t.Callable[["U | Promise[U]"], "Promise[T]"]
```

Lifts a synchronous function to work within the Promise context,
making it return a Promise of the result and allowing it to be used
with Promise inputs.

**Arguments**:

- `func` - A synchronous function that returns a value of type T.
  

**Returns**:

  A function that, when called, returns a Promise wrapping the result of the original function.

<a id="types.monads.Lazy"></a>

## Lazy Objects

```python
class Lazy(Monad[T])
```

<a id="types.monads.Lazy.__init__"></a>

#### \_\_init\_\_

```python
def __init__(computation: t.Callable[[], T]) -> None
```

Initializes a Lazy monad with a computation that will be executed lazily.

**Arguments**:

- `computation` - A function that takes no arguments and returns a value of type T.

<a id="types.monads.Lazy.pure"></a>

#### pure

```python
@classmethod
def pure(cls, value: T) -> "Lazy[T]"
```

Creates a Lazy monad with a pure value.

<a id="types.monads.Lazy.evaluate"></a>

#### evaluate

```python
def evaluate() -> T
```

Evaluates the computation if it has not been evaluated yet and caches the result.

**Returns**:

  The result of the computation.

<a id="types.monads.Lazy.bind"></a>

#### bind

```python
def bind(func: t.Callable[[T], "Lazy[U]"]) -> "Lazy[U]"
```

Lazily applies a function to the result of the current computation.

**Arguments**:

- `func` - A function that takes a value of type T and returns a Lazy monad containing a value of type U.
  

**Returns**:

  A new Lazy monad containing the result of the computation after applying the function.

<a id="types.monads.Lazy.map"></a>

#### map

```python
def map(func: t.Callable[[T], U]) -> "Lazy[U]"
```

Lazily applies a mapping function to the result of the computation.

**Arguments**:

- `func` - A function that takes a value of type T and returns a value of type U.
  

**Returns**:

  A new Lazy monad containing the result of the computation after applying the function.

<a id="types.monads.Lazy.filter"></a>

#### filter

```python
def filter(predicate: t.Callable[[T], bool]) -> "Lazy[T]"
```

Lazily filters the result of the computation based on a predicate.

**Arguments**:

- `predicate` - A function that takes a value of type T and returns a boolean.
  

**Returns**:

  A new Lazy monad containing the result of the computation if the predicate holds.

<a id="types.monads.Lazy.unwrap"></a>

#### unwrap

```python
def unwrap() -> T
```

Forces evaluation of the computation and returns its result.

**Returns**:

  The result of the computation.

<a id="types.monads.Lazy.unwrap_or"></a>

#### unwrap\_or

```python
def unwrap_or(default: T) -> T
```

Tries to evaluate the computation, returning a default value if evaluation raises an exception.

**Arguments**:

- `default` - The value to return if the computation raises an exception.
  

**Returns**:

  The result of the computation or the default value if an exception is raised.

<a id="types.monads.Lazy.lift"></a>

#### lift

```python
@classmethod
def lift(cls, func: t.Callable[[U],
                               T]) -> t.Callable[["U | Lazy[U]"], "Lazy[T]"]
```

Transforms a function to work with arguments and output wrapped in Lazy monads.

**Arguments**:

- `func` - A function that takes any number of arguments and returns a value of type U.
  

**Returns**:

  A function that takes the same number of Lazy-wrapped arguments and returns a Lazy-wrapped result.

<a id="types.monads.Defer"></a>

#### Defer

Defer is an alias for Lazy

<a id="types.monads.S"></a>

#### S

State type

<a id="types.monads.A"></a>

#### A

Return type

<a id="types.monads.B"></a>

#### B

Transformed type

<a id="types.monads.State"></a>

## State Objects

```python
class State(t.Generic[S, A], Monad[A], abc.ABC)
```

<a id="types.monads.State.lift"></a>

#### lift

```python
@classmethod
def lift(
    cls,
    func: t.Callable[[U],
                     A]) -> t.Callable[["U | State[S, U]"], "State[S, A]"]
```

Lifts a function to work within the State monad.

**Arguments**:

- `func` - A function to lift.

**Returns**:

  A new function that returns a State value.

<a id="types.monads.error"></a>

#### error

noqa: E731

<a id="core"></a>

# core

<a id="core.configuration"></a>

# core.configuration

Configuration utilities for the CDF configuration resolver system.

There are 3 ways to request configuration values:

1. Using a Request annotation:

Pro: It's explicit and re-usable. An annotation can be used in multiple places.

```python
import typing as t
import cdf.core.configuration as conf

def foo(bar: t.Annotated[str, conf.Request["api.key"]]) -> None:
    print(bar)
```

2. Setting a __cdf_resolve__ attribute on a callable object. This can be done
directly or by using the `map_section` or `map_values` decorators:

Pro: It's concise and can be used in a decorator. It also works with classes.

```python
import cdf.core.configuration as conf

@conf.map_section("api")
def foo(key: str) -> None:
    print(key)

@conf.map_values(key="api.key")
def bar(key: str) -> None:
    print(key)

def baz(key: str) -> None:
    print(key)

baz.__cdf_resolve__ = ("api",)
```

3. Using the `_cdf_resolve` kwarg to request the resolver:

Pro: It's flexible and can be used in any function. It requires no imports.

```python
def foo(key: str, _cdf_resolve=("api",)) -> None:
    print(key)

def bar(key: str, _cdf_resolve={"key": "api.key"}) -> None:
    print(key)
```

<a id="core.configuration.load_file"></a>

#### load\_file

```python
def load_file(path: Path) -> M.Result[t.Dict[str, t.Any], Exception]
```

Load a configuration from a file path.

**Arguments**:

- `path` - The file path.
  

**Returns**:

  A Result monad with the configuration dictionary if the file format is JSON, YAML or TOML.
  Otherwise, a Result monad with an error.

<a id="core.configuration.add_custom_converter"></a>

#### add\_custom\_converter

```python
def add_custom_converter(name: str, converter: t.Callable[[str],
                                                          t.Any]) -> None
```

Add a custom converter to the configuration system.

<a id="core.configuration.get_converter"></a>

#### get\_converter

```python
def get_converter(name: str) -> t.Callable[[str], t.Any]
```

Get a custom converter from the configuration system.

<a id="core.configuration.remove_converter"></a>

#### remove\_converter

```python
def remove_converter(name: str) -> None
```

Remove a custom converter from the configuration system.

<a id="core.configuration.apply_converters"></a>

#### apply\_converters

```python
def apply_converters(input_value: t.Any,
                     resolver: t.Optional["ConfigResolver"] = None) -> t.Any
```

Apply converters to a string.

<a id="core.configuration._ConfigScopes"></a>

## \_ConfigScopes Objects

```python
class _ConfigScopes(t.NamedTuple)
```

A struct to store named configuration scopes by precedence.

<a id="core.configuration._ConfigScopes.explicit"></a>

#### explicit

User-provided configuration passed as a dictionary.

<a id="core.configuration._ConfigScopes.environment"></a>

#### environment

Environment-specific configuration loaded from a file.

<a id="core.configuration._ConfigScopes.baseline"></a>

#### baseline

Configuration loaded from a base config file.

<a id="core.configuration._ConfigScopes.resolve"></a>

#### resolve

```python
def resolve() -> "Box"
```

Resolve the configuration scopes.

<a id="core.configuration.ConfigLoader"></a>

## ConfigLoader Objects

```python
class ConfigLoader()
```

Load configuration from multiple sources.

<a id="core.configuration.ConfigLoader.__init__"></a>

#### \_\_init\_\_

```python
def __init__(*sources: ConfigSource,
             environment: str = "dev",
             deferred: bool = False) -> None
```

Initialize the configuration loader.

<a id="core.configuration.ConfigLoader.config"></a>

#### config

```python
@property
def config() -> t.Mapping[str, t.Any]
```

Get the configuration dictionary.

<a id="core.configuration.ConfigLoader.import_"></a>

#### import\_

```python
def import_(source: ConfigSource, append: bool = True) -> None
```

Include a new source of configuration.

<a id="core.configuration.RESOLVER_HINT"></a>

#### RESOLVER\_HINT

A hint to engage the configuration resolver.

<a id="core.configuration.map_config_section"></a>

#### map\_config\_section

```python
def map_config_section(
        *sections: str) -> t.Callable[[t.Callable[P, T]], t.Callable[P, T]]
```

Mark a function to inject configuration values from a specific section.

<a id="core.configuration.map_config_values"></a>

#### map\_config\_values

```python
def map_config_values(**mapping: t.Any
                      ) -> t.Callable[[t.Callable[P, T]], t.Callable[P, T]]
```

Mark a function to inject configuration values from a specific mapping of param names to keys.

<a id="core.configuration.ConfigResolver"></a>

## ConfigResolver Objects

```python
class ConfigResolver(t.MutableMapping)
```

Resolve configuration values.

<a id="core.configuration.ConfigResolver.map_section"></a>

#### map\_section

Mark a function to inject configuration values from a specific section.

<a id="core.configuration.ConfigResolver.map_values"></a>

#### map\_values

Mark a function to inject configuration values from a specific mapping of param names to keys.

<a id="core.configuration.ConfigResolver.__init__"></a>

#### \_\_init\_\_

```python
def __init__(
    *sources: ConfigSource,
    environment: str = "dev",
    loader: ConfigLoader = ConfigLoader("config.json")
) -> None
```

Initialize the configuration resolver.

<a id="core.configuration.ConfigResolver.config"></a>

#### config

```python
@property
def config() -> t.Mapping[str, t.Any]
```

Get the configuration dictionary.

<a id="core.configuration.ConfigResolver.__getitem__"></a>

#### \_\_getitem\_\_

```python
def __getitem__(key: str) -> t.Any
```

Get a configuration value.

<a id="core.configuration.ConfigResolver.__setitem__"></a>

#### \_\_setitem\_\_

```python
def __setitem__(key: str, value: t.Any) -> None
```

Set a configuration value.

<a id="core.configuration.ConfigResolver.__getattr__"></a>

#### \_\_getattr\_\_

```python
def __getattr__(key: str) -> t.Any
```

Get a configuration value.

<a id="core.configuration.ConfigResolver.__enter__"></a>

#### \_\_enter\_\_

```python
def __enter__() -> "ConfigResolver"
```

Enter a context.

<a id="core.configuration.ConfigResolver.__exit__"></a>

#### \_\_exit\_\_

```python
def __exit__(*args) -> None
```

Exit a context.

<a id="core.configuration.ConfigResolver.__repr__"></a>

#### \_\_repr\_\_

```python
def __repr__() -> str
```

Get a string representation of the configuration resolver.

<a id="core.configuration.ConfigResolver.set_environment"></a>

#### set\_environment

```python
def set_environment(environment: str) -> None
```

Set the environment of the configuration resolver.

<a id="core.configuration.ConfigResolver.import_source"></a>

#### import\_source

```python
def import_source(source: ConfigSource, append: bool = True) -> None
```

Include a new source of configuration.

<a id="core.configuration.ConfigResolver.kwarg_hint"></a>

#### kwarg\_hint

A hint supplied in a kwarg to engage the configuration resolver.

<a id="core.configuration.ConfigResolver.resolve_defaults"></a>

#### resolve\_defaults

```python
def resolve_defaults(func_or_cls: t.Callable[P, T]) -> t.Callable[..., T]
```

Resolve configuration values into a function or class.

<a id="core.configuration.ConfigResolver.is_resolvable"></a>

#### is\_resolvable

```python
def is_resolvable(param: inspect.Parameter) -> bool
```

Check if a parameter is injectable.

<a id="core.configuration.ConfigResolver.extract_request_annotation"></a>

#### extract\_request\_annotation

```python
@staticmethod
def extract_request_annotation(param: inspect.Parameter) -> t.Optional[str]
```

Extract a request annotation from a parameter.

<a id="core.configuration.ConfigResolver.__call__"></a>

#### \_\_call\_\_

```python
def __call__(func_or_cls: t.Callable[P, T], *args: t.Any,
             **kwargs: t.Any) -> T
```

Invoke a callable with injected configuration values.

<a id="core.component.service"></a>

# core.component.service

<a id="core.component.service.Service"></a>

## Service Objects

```python
class Service(Component[ServiceProto])
```

A service that the workspace provides. IE an API, database, requests client, etc.

<a id="core.component"></a>

# core.component

<a id="core.component.operation"></a>

# core.component.operation

<a id="core.component.operation.Operation"></a>

## Operation Objects

```python
class Operation(Entrypoint[OperationProto])
```

A generic callable that returns an exit code.

<a id="core.component.pipeline"></a>

# core.component.pipeline

<a id="core.component.pipeline.DataPipeline"></a>

## DataPipeline Objects

```python
class DataPipeline(Entrypoint[DataPipelineProto])
```

A data pipeline which loads data from a source to a destination.

<a id="core.component.pipeline.DataPipeline.__call__"></a>

#### \_\_call\_\_

```python
def __call__(*args: t.Any, **kwargs: t.Any) -> t.List["LoadInfo"]
```

Run the data pipeline

<a id="core.component.pipeline.DataPipeline.get_schemas"></a>

#### get\_schemas

```python
def get_schemas(destination: t.Optional["DltDestination"] = None)
```

Get the schemas for the pipeline.

<a id="core.component.pipeline.DataPipeline.run_tests"></a>

#### run\_tests

```python
def run_tests() -> None
```

Run the integration test for the pipeline.

<a id="core.component.publisher"></a>

# core.component.publisher

<a id="core.component.publisher.DataPublisher"></a>

## DataPublisher Objects

```python
class DataPublisher(Entrypoint[DataPublisherProto])
```

A data publisher which pushes data to an operational system.

<a id="core.component.publisher.DataPublisher.__call__"></a>

#### \_\_call\_\_

```python
def __call__(*args: t.Any, **kwargs: t.Any) -> None
```

Publish the data

<a id="core.component.base"></a>

# core.component.base

<a id="core.component.base.ServiceLevelAgreement"></a>

## ServiceLevelAgreement Objects

```python
class ServiceLevelAgreement(Enum)
```

An SLA to assign to a component. Users can define the meaning of each level.

<a id="core.component.base._Node"></a>

## \_Node Objects

```python
class _Node(pydantic.BaseModel)
```

A node in a graph of components.

<a id="core.component.base._Node.owner"></a>

#### owner

The owner of the node. Useful for tracking who to contact for issues or config.

<a id="core.component.base._Node.description"></a>

#### description

A description of the node.

<a id="core.component.base._Node.sla"></a>

#### sla

The SLA for the node.

<a id="core.component.base._Node.enabled"></a>

#### enabled

Whether the node is enabled or disabled. Disabled components are not loaded.

<a id="core.component.base._Node.version"></a>

#### version

A semantic version for the node. Can signal breaking changes to dependents.

<a id="core.component.base._Node.tags"></a>

#### tags

Tags to categorize the node.

<a id="core.component.base._Node.metadata"></a>

#### metadata

Additional metadata for the node. Useful for custom integrations.

<a id="core.component.base.Component"></a>

## Component Objects

```python
class Component(_Node, t.Generic[T])
```

A component with a binding to a dependency.

<a id="core.component.base.Component.main"></a>

#### main

The dependency for the component. This is what is injected into the workspace.

<a id="core.component.base.Component.name"></a>

#### name

The key to register the component in the container.

Must be a valid Python identifier. Users can use these names as function parameters
for implicit dependency injection. Names must be unique within the workspace.

<a id="core.component.base.Component.__call__"></a>

#### \_\_call\_\_

```python
def __call__() -> T
```

Unwrap the main dependency invoking the underlying callable.

<a id="core.component.base.Entrypoint"></a>

## Entrypoint Objects

```python
class Entrypoint(_Node, t.Generic[T])
```

An entrypoint representing an invokeable set of functions.

<a id="core.component.base.Entrypoint.main"></a>

#### main

The main function associated with the entrypoint.

<a id="core.component.base.Entrypoint.name"></a>

#### name

The name of the entrypoint.

This is used to register the entrypoint in the workspace and CLI. Names must be
unique within the workspace. The name can contain spaces and special characters.

<a id="core.component.base.Entrypoint.__call__"></a>

#### \_\_call\_\_

```python
def __call__(*args: t.Any, **kwargs: t.Any) -> t.Any
```

Invoke the entrypoint.

<a id="core.context"></a>

# core.context

Context management utilities for managing the active workspace.

<a id="core.context.get_active_workspace"></a>

#### get\_active\_workspace

```python
def get_active_workspace() -> t.Optional["Workspace"]
```

Get the active workspace for resolving injected dependencies.

<a id="core.context.set_active_workspace"></a>

#### set\_active\_workspace

```python
def set_active_workspace(workspace: t.Optional["Workspace"]) -> Token
```

Set the active workspace for resolving injected dependencies.

<a id="core.context.use_workspace"></a>

#### use\_workspace

```python
@contextlib.contextmanager
def use_workspace(workspace: t.Optional["Workspace"]) -> t.Iterator[None]
```

Context manager for temporarily setting the active workspace.

<a id="core.context.resolve"></a>

#### resolve

```python
def resolve(
    dependencies: t.Union[t.Callable[..., T], bool] = True,
    configuration: bool = True,
    eagerly_bind_workspace: bool = False
) -> t.Callable[..., t.Union[T, t.Callable[..., T]]]
```

Decorator for injecting dependencies and resolving configuration for a function.

<a id="core.context.get_default_callable_lifecycle"></a>

#### get\_default\_callable\_lifecycle

```python
def get_default_callable_lifecycle() -> t.Optional["Lifecycle"]
```

Get the default lifecycle for callables when otherwise unspecified.

<a id="core.context.set_default_callable_lifecycle"></a>

#### set\_default\_callable\_lifecycle

```python
def set_default_callable_lifecycle(
        lifecycle: t.Optional["Lifecycle"]) -> Token
```

Set the default lifecycle for callables when otherwise unspecified.

<a id="core.context.use_default_callable_lifecycle"></a>

#### use\_default\_callable\_lifecycle

```python
@contextlib.contextmanager
def use_default_callable_lifecycle(
        lifecycle: t.Optional["Lifecycle"]) -> t.Iterator[None]
```

Context manager for temporarily setting the default callable lifecycle.

<a id="core.workspace"></a>

# core.workspace

A workspace is a container for components and configurations.

<a id="core.workspace.Workspace"></a>

## Workspace Objects

```python
class Workspace(pydantic.BaseModel)
```

A CDF workspace that allows for dependency injection and configuration resolution.

<a id="core.workspace.Workspace.name"></a>

#### name

A human-readable name for the workspace.

<a id="core.workspace.Workspace.version"></a>

#### version

A semver version string for the workspace.

<a id="core.workspace.Workspace.environment"></a>

#### environment

The runtime environment used to resolve configuration.

<a id="core.workspace.Workspace.conf_resolver"></a>

#### conf\_resolver

The configuration resolver for the workspace.

<a id="core.workspace.Workspace.container"></a>

#### container

The dependency injection container for the workspace.

<a id="core.workspace.Workspace.configuration_sources"></a>

#### configuration\_sources

A list of configuration sources resolved and merged by the workspace.

<a id="core.workspace.Workspace.service_definitions"></a>

#### service\_definitions

An iterable of raw service definitions that the workspace provides.

<a id="core.workspace.Workspace.pipeline_definitions"></a>

#### pipeline\_definitions

An iterable of raw pipeline definitions that the workspace provides.

<a id="core.workspace.Workspace.publishers_definitions"></a>

#### publishers\_definitions

An iterable of raw publisher definitions that the workspace provides.

<a id="core.workspace.Workspace.operation_definitions"></a>

#### operation\_definitions

An iterable of raw generic operation definitions that the workspace provides.

<a id="core.workspace.Workspace.sqlmesh_path"></a>

#### sqlmesh\_path

The path to the sqlmesh root for the workspace.

<a id="core.workspace.Workspace.sqlmesh_context_kwargs"></a>

#### sqlmesh\_context\_kwargs

Keyword arguments to pass to the sqlmesh context.

<a id="core.workspace.Workspace.activate"></a>

#### activate

```python
def activate() -> Self
```

Activate the workspace for the current context.

<a id="core.workspace.Workspace.services"></a>

#### services

```python
@cached_property
def services() -> t.Dict[str, cmp.Service]
```

Return the resolved services of the workspace.

<a id="core.workspace.Workspace.pipelines"></a>

#### pipelines

```python
@cached_property
def pipelines() -> t.Dict[str, cmp.DataPipeline]
```

Return the resolved data pipelines of the workspace.

<a id="core.workspace.Workspace.publishers"></a>

#### publishers

```python
@cached_property
def publishers() -> t.Dict[str, cmp.DataPublisher]
```

Return the resolved data publishers of the workspace.

<a id="core.workspace.Workspace.operations"></a>

#### operations

```python
@cached_property
def operations() -> t.Dict[str, cmp.Operation]
```

Return the resolved operations of the workspace.

<a id="core.workspace.Workspace.get_sqlmesh_context"></a>

#### get\_sqlmesh\_context

```python
def get_sqlmesh_context(gateway: t.Optional[str] = None,
                        must_exist: bool = False,
                        **kwargs: t.Any) -> t.Optional["sqlmesh.Context"]
```

Return the transform context or raise an error if not defined.

<a id="core.workspace.Workspace.cli"></a>

#### cli

```python
@property
def cli() -> "click.Group"
```

Dynamically generate a CLI entrypoint for the workspace.

<a id="core.workspace.Workspace.bind"></a>

#### bind

```python
def bind(func_or_cls: t.Callable[P, T]) -> t.Callable[..., T]
```

Wrap a function with configuration and dependencies defined in the workspace.

<a id="core.workspace.Workspace.invoke"></a>

#### invoke

```python
def invoke(func_or_cls: t.Callable[P, T], *args: t.Any, **kwargs: t.Any) -> T
```

Invoke a function with configuration and dependencies defined in the workspace.

<a id="core.injector.registry"></a>

# core.injector.registry

Dependency registry with lifecycle management.

<a id="core.injector.registry.Lifecycle"></a>

## Lifecycle Objects

```python
class Lifecycle(enum.Enum)
```

Lifecycle of a dependency.

<a id="core.injector.registry.Lifecycle.PROTOTYPE"></a>

#### PROTOTYPE

A prototype dependency is created every time it is requested

<a id="core.injector.registry.Lifecycle.SINGLETON"></a>

#### SINGLETON

A singleton dependency is created once and shared.

<a id="core.injector.registry.Lifecycle.INSTANCE"></a>

#### INSTANCE

An instance dependency is a global object which is not created by the container.

<a id="core.injector.registry.Lifecycle.is_prototype"></a>

#### is\_prototype

```python
@property
def is_prototype() -> bool
```

Check if the lifecycle is prototype.

<a id="core.injector.registry.Lifecycle.is_singleton"></a>

#### is\_singleton

```python
@property
def is_singleton() -> bool
```

Check if the lifecycle is singleton.

<a id="core.injector.registry.Lifecycle.is_instance"></a>

#### is\_instance

```python
@property
def is_instance() -> bool
```

Check if the lifecycle is instance.

<a id="core.injector.registry.Lifecycle.is_deferred"></a>

#### is\_deferred

```python
@property
def is_deferred() -> bool
```

Check if the object to be created is deferred.

<a id="core.injector.registry.TypedKey"></a>

## TypedKey Objects

```python
class TypedKey(t.NamedTuple)
```

A key which is a tuple of a name and a type.

<a id="core.injector.registry.TypedKey.type_name"></a>

#### type\_name

```python
@property
def type_name() -> t.Optional[str]
```

Get the name of the type if applicable.

<a id="core.injector.registry.TypedKey.__eq__"></a>

#### \_\_eq\_\_

```python
def __eq__(other: t.Any) -> bool
```

Two keys are equal if their names and base types match.

<a id="core.injector.registry.TypedKey.__hash__"></a>

#### \_\_hash\_\_

```python
def __hash__() -> int
```

Hash the key with the effective type if possible.

<a id="core.injector.registry.DependencyKey"></a>

#### DependencyKey

A string or a typed key.

<a id="core.injector.registry.Dependency"></a>

## Dependency Objects

```python
class Dependency(pydantic.BaseModel, t.Generic[T])
```

A Monadic type which wraps a value with lifecycle and allows simple transformations.

<a id="core.injector.registry.Dependency.factory"></a>

#### factory

The factory or instance of the dependency.

<a id="core.injector.registry.Dependency.lifecycle"></a>

#### lifecycle

The lifecycle of the dependency.

<a id="core.injector.registry.Dependency.conf_spec"></a>

#### conf\_spec

A hint for configuration values.

<a id="core.injector.registry.Dependency.alias"></a>

#### alias

Used as an alternative to inferring the name from the factory.

<a id="core.injector.registry.Dependency.instance"></a>

#### instance

```python
@classmethod
def instance(cls, instance: t.Any) -> "Dependency"
```

Create a dependency from an instance.

**Arguments**:

- `instance` - The instance to use as the dependency.
  

**Returns**:

  A new Dependency object with the instance lifecycle.

<a id="core.injector.registry.Dependency.singleton"></a>

#### singleton

```python
@classmethod
def singleton(cls, factory: t.Callable[..., T], *args: t.Any,
              **kwargs: t.Any) -> "Dependency"
```

Create a singleton dependency.

**Arguments**:

- `factory` - The factory function to create the dependency.
- `args` - Positional arguments to pass to the factory.
- `kwargs` - Keyword arguments to pass to the factory.
  

**Returns**:

  A new Dependency object with the singleton lifecycle.

<a id="core.injector.registry.Dependency.prototype"></a>

#### prototype

```python
@classmethod
def prototype(cls, factory: t.Callable[..., T], *args: t.Any,
              **kwargs: t.Any) -> "Dependency"
```

Create a prototype dependency.

**Arguments**:

- `factory` - The factory function to create the dependency.
- `args` - Positional arguments to pass to the factory.
- `kwargs` - Keyword arguments to pass to the factory.
  

**Returns**:

  A new Dependency object with the prototype lifecycle.

<a id="core.injector.registry.Dependency.wrap"></a>

#### wrap

```python
@classmethod
def wrap(cls, obj: t.Any, *args: t.Any, **kwargs: t.Any) -> Self
```

Wrap an object as a dependency.

Assumes singleton lifecycle for callables unless a default lifecycle context is set.

**Arguments**:

- `obj` - The object to wrap.
  

**Returns**:

  A new Dependency object with the object as the factory.

<a id="core.injector.registry.Dependency.map_value"></a>

#### map\_value

```python
def map_value(func: t.Callable[[T], T]) -> Self
```

Apply a function to the unwrapped value.

**Arguments**:

- `func` - The function to apply to the unwrapped value.
  

**Returns**:

  A new Dependency object with the function applied.

<a id="core.injector.registry.Dependency.map"></a>

#### map

```python
def map(*funcs: t.Callable[[t.Callable[..., T]], t.Callable[..., T]],
        idempotent: bool = False) -> Self
```

Apply a sequence of transformations to the wrapped value.

The transformations are applied in order. This is a no-op if the dependency is
already resolved and idempotent is True or the dependency is an instance.

**Arguments**:

- `funcs` - The functions to apply to the wrapped value.
- `idempotent` - If True, allow transformations on resolved dependencies to be a no-op.
  

**Returns**:

  The Dependency object with the transformations applied.

<a id="core.injector.registry.Dependency.unwrap"></a>

#### unwrap

```python
def unwrap() -> T
```

Unwrap the value from the factory.

<a id="core.injector.registry.Dependency.__call__"></a>

#### \_\_call\_\_

```python
def __call__() -> T
```

Alias for unwrap.

<a id="core.injector.registry.Dependency.try_infer_type"></a>

#### try\_infer\_type

```python
def try_infer_type() -> t.Optional[t.Type[T]]
```

Get the effective type of the dependency.

<a id="core.injector.registry.Dependency.try_infer_name"></a>

#### try\_infer\_name

```python
def try_infer_name() -> t.Optional[str]
```

Infer the name of the dependency from the factory.

<a id="core.injector.registry.Dependency.generate_key"></a>

#### generate\_key

```python
def generate_key(
        name: t.Optional[DependencyKey] = None) -> t.Union[str, TypedKey]
```

Generate a typed key for the dependency.

**Arguments**:

- `name` - The name of the dependency.
  

**Returns**:

  A typed key if the type can be inferred, else the name.

<a id="core.injector.registry.DependencyRegistry"></a>

## DependencyRegistry Objects

```python
class DependencyRegistry(t.MutableMapping[DependencyKey, Dependency])
```

A registry for dependencies with lifecycle management.

Dependencies can be registered with a name or a typed key. Typed keys are tuples
of a name and a type hint. Dependencies can be added with a lifecycle, which can be
one of prototype, singleton, or instance. Dependencies can be retrieved by name or
typed key. Dependencies can be injected into functions or classes. Dependencies can
be wired into callables to resolve a dependency graph.

<a id="core.injector.registry.DependencyRegistry.__init__"></a>

#### \_\_init\_\_

```python
def __init__(strict: bool = False) -> None
```

Initialize the registry.

**Arguments**:

- `strict` - If True, do not inject an untyped lookup for a typed dependency.

<a id="core.injector.registry.DependencyRegistry.dependencies"></a>

#### dependencies

```python
@property
def dependencies() -> ChainMap[t.Any, Dependency]
```

Get all dependencies.

<a id="core.injector.registry.DependencyRegistry.add"></a>

#### add

```python
def add(key: DependencyKey,
        value: t.Any,
        lifecycle: t.Optional[Lifecycle] = None,
        override: bool = False,
        init_args: t.Tuple[t.Any, ...] = (),
        init_kwargs: t.Optional[t.Dict[str, t.Any]] = None) -> None
```

Register a dependency with the container.

**Arguments**:

- `key` - The name of the dependency.
- `value` - The factory or instance of the dependency.
- `lifecycle` - The lifecycle of the dependency.
- `override` - If True, override an existing dependency.
- `init_args` - Arguments to initialize the factory with.
- `init_kwargs` - Keyword arguments to initialize the factory with.

<a id="core.injector.registry.DependencyRegistry.add_from_dependency"></a>

#### add\_from\_dependency

```python
def add_from_dependency(dependency: Dependency,
                        key: t.Optional[DependencyKey] = None,
                        override: bool = False) -> None
```

Add a Dependency object to the container.

**Arguments**:

- `key` - The name or typed key of the dependency.
- `dependency` - The dependency object.
- `override` - If True, override an existing dependency

<a id="core.injector.registry.DependencyRegistry.remove"></a>

#### remove

```python
def remove(name_or_key: DependencyKey) -> None
```

Remove a dependency by name or key from the container.

**Arguments**:

- `name_or_key` - The name or typed key of the dependency.

<a id="core.injector.registry.DependencyRegistry.clear"></a>

#### clear

```python
def clear() -> None
```

Clear all dependencies and singletons.

<a id="core.injector.registry.DependencyRegistry.has"></a>

#### has

```python
def has(name_or_key: DependencyKey) -> bool
```

Check if a dependency is registered.

**Arguments**:

- `name_or_key` - The name or typed key of the dependency.

<a id="core.injector.registry.DependencyRegistry.resolve"></a>

#### resolve

```python
def resolve(name_or_key: DependencyKey, must_exist: bool = False) -> t.Any
```

Get a dependency.

**Arguments**:

- `name_or_key` - The name or typed key of the dependency.
- `must_exist` - If True, raise KeyError if the dependency is not found.
  

**Returns**:

  The dependency if found, else None.

<a id="core.injector.registry.DependencyRegistry.__contains__"></a>

#### \_\_contains\_\_

```python
def __contains__(name: t.Any) -> bool
```

Check if a dependency is registered.

<a id="core.injector.registry.DependencyRegistry.__getitem__"></a>

#### \_\_getitem\_\_

```python
def __getitem__(name: DependencyKey) -> t.Any
```

Get a dependency. Raises KeyError if not found.

<a id="core.injector.registry.DependencyRegistry.__setitem__"></a>

#### \_\_setitem\_\_

```python
def __setitem__(name: DependencyKey, value: t.Any) -> None
```

Add a dependency. Defaults to singleton lifecycle if callable, else instance.

<a id="core.injector.registry.DependencyRegistry.__delitem__"></a>

#### \_\_delitem\_\_

```python
def __delitem__(name: DependencyKey) -> None
```

Remove a dependency.

<a id="core.injector.registry.DependencyRegistry.wire"></a>

#### wire

```python
def wire(func_or_cls: t.Callable[P, T]) -> t.Callable[..., T]
```

Inject dependencies into a function.

**Arguments**:

- `func_or_cls` - The function or class to inject dependencies into.
  

**Returns**:

  A function that can be called with dependencies injected

<a id="core.injector.registry.DependencyRegistry.__call__"></a>

#### \_\_call\_\_

```python
def __call__(func_or_cls: t.Callable[P, T], *args: t.Any,
             **kwargs: t.Any) -> T
```

Invoke a callable with dependencies injected from the registry.

**Arguments**:

- `func_or_cls` - The function or class to invoke.
- `args` - Positional arguments to pass to the callable.
- `kwargs` - Keyword arguments to pass to the callable.
  

**Returns**:

  The result of the callable

<a id="core.injector.registry.DependencyRegistry.__iter__"></a>

#### \_\_iter\_\_

```python
def __iter__() -> t.Iterator[TypedKey]
```

Iterate over dependency names.

<a id="core.injector.registry.DependencyRegistry.__len__"></a>

#### \_\_len\_\_

```python
def __len__() -> int
```

Return the number of dependencies.

<a id="core.injector.registry.DependencyRegistry.__bool__"></a>

#### \_\_bool\_\_

```python
def __bool__() -> bool
```

True if the registry has dependencies.

<a id="core.injector.registry.DependencyRegistry.__or__"></a>

#### \_\_or\_\_

```python
def __or__(other: "DependencyRegistry") -> "DependencyRegistry"
```

Merge two registries like pythons dict union overload.

<a id="core.injector.registry.DependencyRegistry.__getstate__"></a>

#### \_\_getstate\_\_

```python
def __getstate__() -> t.Dict[str, t.Any]
```

Serialize the state.

<a id="core.injector.registry.DependencyRegistry.__setstate__"></a>

#### \_\_setstate\_\_

```python
def __setstate__(state: t.Dict[str, t.Any]) -> None
```

Deserialize the state.

<a id="core.injector.registry.GLOBAL_REGISTRY"></a>

#### GLOBAL\_REGISTRY

A global dependency registry.

<a id="core.injector"></a>

# core.injector

<a id="core.injector.errors"></a>

# core.injector.errors

<a id="core.injector.errors.DependencyCycleError"></a>

## DependencyCycleError Objects

```python
class DependencyCycleError(Exception)
```

Raised when a dependency cycle is detected.

<a id="core.injector.errors.DependencyMutationError"></a>

## DependencyMutationError Objects

```python
class DependencyMutationError(Exception)
```

Raised when an instance/singleton dependency has already been resolved but a mutation is attempted.

<a id="proxy"></a>

# proxy

The proxy module provides a MySQL proxy server for the CDF.

The proxy server is used to intercept MySQL queries and execute them using SQLMesh.
This allows it to integrate with BI tools and other MySQL clients. Furthermore,
during interception, the server can rewrite queries expanding semantic references
making it an easy to use semantic layer for SQLMesh.

<a id="proxy.planner"></a>

# proxy.planner

An http server which executed a plan which is a pickled pydantic model

This is purely a POC. It will be replaced by a more robust solution in the future
using flask or fastapi. It will always be designed such that input must be
trusted. In an environment where the input is not trusted, the server should
never be exposed to the internet. It should always be behind a firewall and
only accessible by trusted clients.

<a id="proxy.planner.run_plan_server"></a>

#### run\_plan\_server

```python
def run_plan_server(port: int, context: sqlmesh.Context) -> None
```

Listen on a port and execute plans.

<a id="proxy.mysql"></a>

# proxy.mysql

A MySQL proxy server which uses SQLMesh to execute queries.

<a id="proxy.mysql.file_watcher"></a>

#### file\_watcher

```python
async def file_watcher(context: sqlmesh.Context) -> None
```

Watch for changes in the workspace and refresh the context.

<a id="proxy.mysql.SQLMeshSession"></a>

## SQLMeshSession Objects

```python
class SQLMeshSession(Session)
```

A session for the MySQL proxy server which uses SQLMesh.

<a id="proxy.mysql.SQLMeshSession.query"></a>

#### query

```python
async def query(
    expression: exp.Expression, sql: str,
    attrs: t.Dict[str,
                  str]) -> t.Tuple[t.Tuple[t.Tuple[t.Any], ...], t.List[str]]
```

Execute a query.

<a id="proxy.mysql.SQLMeshSession.schema"></a>

#### schema

```python
async def schema() -> t.Dict[str, t.Dict[str, t.Dict[str, str]]]
```

Get the schema of the database.

<a id="proxy.mysql.run_mysql_proxy"></a>

#### run\_mysql\_proxy

```python
async def run_mysql_proxy(context: sqlmesh.Context) -> None
```

Run the MySQL proxy server.

<a id="integrations"></a>

# integrations

<a id="integrations.slack"></a>

# integrations.slack

<a id="integrations.slack.SlackMessageComposer"></a>

## SlackMessageComposer Objects

```python
class SlackMessageComposer()
```

Builds Slack message with primary and secondary blocks

<a id="integrations.slack.SlackMessageComposer.__init__"></a>

#### \_\_init\_\_

```python
def __init__(initial_message: t.Optional[TSlackMessage] = None) -> None
```

Initialize the Slack message builder

<a id="integrations.slack.SlackMessageComposer.add_primary_blocks"></a>

#### add\_primary\_blocks

```python
def add_primary_blocks(*blocks: TSlackBlock) -> "SlackMessageComposer"
```

Add blocks to the message. Blocks are always displayed

<a id="integrations.slack.SlackMessageComposer.add_secondary_blocks"></a>

#### add\_secondary\_blocks

```python
def add_secondary_blocks(*blocks: TSlackBlock) -> "SlackMessageComposer"
```

Add attachments to the message

Attachments are hidden behind "show more" button. The first 5 attachments
are always displayed. NOTICE: attachments blocks are deprecated by Slack

<a id="integrations.slack.normalize_message"></a>

#### normalize\_message

```python
def normalize_message(
        message: t.Union[str, t.List[str], t.Iterable[str]]) -> str
```

Normalize message to fit Slack's max text length

<a id="integrations.slack.divider_block"></a>

#### divider\_block

```python
def divider_block() -> dict
```

Create a divider block

<a id="integrations.slack.fields_section_block"></a>

#### fields\_section\_block

```python
def fields_section_block(*messages: str) -> dict
```

Create a section block with multiple fields

<a id="integrations.slack.text_section_block"></a>

#### text\_section\_block

```python
def text_section_block(message: str) -> dict
```

Create a section block with text

<a id="integrations.slack.empty_section_block"></a>

#### empty\_section\_block

```python
def empty_section_block() -> dict
```

Create an empty section block

<a id="integrations.slack.context_block"></a>

#### context\_block

```python
def context_block(*messages: str) -> dict
```

Create a context block with multiple fields

<a id="integrations.slack.header_block"></a>

#### header\_block

```python
def header_block(message: str) -> dict
```

Create a header block

<a id="integrations.slack.button_action_block"></a>

#### button\_action\_block

```python
def button_action_block(text: str, url: str) -> dict
```

Create a button action block

<a id="integrations.slack.compacted_sections_blocks"></a>

#### compacted\_sections\_blocks

```python
def compacted_sections_blocks(
        *messages: t.Union[str, t.Iterable[str]]) -> t.List[dict]
```

Create a list of compacted sections blocks

<a id="integrations.slack.SlackAlertIcon"></a>

## SlackAlertIcon Objects

```python
class SlackAlertIcon(str, Enum)
```

Enum for status of the alert

<a id="integrations.slack.stringify_list"></a>

#### stringify\_list

```python
def stringify_list(list_variation: t.Union[t.List[str], str]) -> str
```

Prettify and deduplicate list of strings converting it to a newline delimited string

<a id="integrations.slack.send_basic_slack_message"></a>

#### send\_basic\_slack\_message

```python
def send_basic_slack_message(incoming_hook: str,
                             message: str,
                             is_markdown: bool = True) -> None
```

Sends a `message` to  Slack `incoming_hook`, by default formatted as markdown.

<a id="integrations.slack.send_extract_start_slack_message"></a>

#### send\_extract\_start\_slack\_message

```python
def send_extract_start_slack_message(incoming_hook: str, source: str,
                                     run_id: str, tags: t.List[str],
                                     owners: t.List[str], environment: str,
                                     resources_selected: t.List[str],
                                     resources_count: int) -> None
```

Sends a Slack message for the start of an extract

<a id="integrations.slack.send_extract_failure_message"></a>

#### send\_extract\_failure\_message

```python
def send_extract_failure_message(incoming_hook: str, source: str, run_id: str,
                                 duration: float, error: Exception) -> None
```

Sends a Slack message for the failure of an extract

<a id="integrations.slack.send_extract_success_message"></a>

#### send\_extract\_success\_message

```python
def send_extract_success_message(incoming_hook: str, source: str, run_id: str,
                                 duration: float) -> None
```

Sends a Slack message for the success of an extract

<a id="integrations.slack.send_normalize_start_slack_message"></a>

#### send\_normalize\_start\_slack\_message

```python
def send_normalize_start_slack_message(incoming_hook: str, source: str,
                                       blob_name: str, run_id: str,
                                       environment: str) -> None
```

Sends a Slack message for the start of an extract

<a id="integrations.slack.send_normalize_failure_message"></a>

#### send\_normalize\_failure\_message

```python
def send_normalize_failure_message(incoming_hook: str, source: str,
                                   blob_name: str, run_id: str,
                                   duration: float, error: Exception) -> None
```

Sends a Slack message for the failure of an normalization

<a id="integrations.slack.send_normalization_success_message"></a>

#### send\_normalization\_success\_message

```python
def send_normalization_success_message(incoming_hook: str, source: str,
                                       blob_name: str, run_id: str,
                                       duration: float) -> None
```

Sends a Slack message for the success of an normalization

<a id="integrations.slack.send_load_start_slack_message"></a>

#### send\_load\_start\_slack\_message

```python
def send_load_start_slack_message(incoming_hook: str, source: str,
                                  destination: str, dataset: str,
                                  run_id: str) -> None
```

Sends a Slack message for the start of a load

<a id="integrations.slack.send_load_failure_message"></a>

#### send\_load\_failure\_message

```python
def send_load_failure_message(incoming_hook: str, source: str,
                              destination: str, dataset: str,
                              run_id: str) -> None
```

Sends a Slack message for the failure of an load

<a id="integrations.slack.send_load_success_message"></a>

#### send\_load\_success\_message

```python
def send_load_success_message(incoming_hook: str, source: str,
                              destination: str, dataset: str, run_id: str,
                              payload: str) -> None
```

Sends a Slack message for the success of an normalization

<a id="integrations.feature_flag.launchdarkly"></a>

# integrations.feature\_flag.launchdarkly

LaunchDarkly feature flag provider.

<a id="integrations.feature_flag.launchdarkly.LaunchDarklyFeatureFlagAdapter"></a>

## LaunchDarklyFeatureFlagAdapter Objects

```python
class LaunchDarklyFeatureFlagAdapter(AbstractFeatureFlagAdapter)
```

A feature flag adapter that uses LaunchDarkly.

<a id="integrations.feature_flag.launchdarkly.LaunchDarklyFeatureFlagAdapter.__init__"></a>

#### \_\_init\_\_

```python
@with_config(sections=("feature_flags", ))
def __init__(sdk_key: str, **kwargs: t.Any) -> None
```

Initialize the LaunchDarkly feature flags.

**Arguments**:

- `sdk_key` - The SDK key to use for LaunchDarkly.

<a id="integrations.feature_flag.harness"></a>

# integrations.feature\_flag.harness

Harness feature flag provider.

<a id="integrations.feature_flag.harness.HarnessFeatureFlagAdapter"></a>

## HarnessFeatureFlagAdapter Objects

```python
class HarnessFeatureFlagAdapter(AbstractFeatureFlagAdapter)
```

<a id="integrations.feature_flag.harness.HarnessFeatureFlagAdapter.__init__"></a>

#### \_\_init\_\_

```python
@with_config(sections=("feature_flags", ))
def __init__(sdk_key: str = dlt.secrets.value,
             api_key: str = dlt.secrets.value,
             account: str = dlt.secrets.value,
             organization: str = dlt.secrets.value,
             project: str = dlt.secrets.value,
             **kwargs: t.Any) -> None
```

Initialize the adapter.

<a id="integrations.feature_flag.harness.HarnessFeatureFlagAdapter.client"></a>

#### client

```python
@property
def client() -> CfClient
```

Get the client and cache it in the instance.

<a id="integrations.feature_flag.harness.HarnessFeatureFlagAdapter.pool"></a>

#### pool

```python
@property
def pool() -> ThreadPoolExecutor
```

Get the thread pool.

<a id="integrations.feature_flag.harness.HarnessFeatureFlagAdapter.get"></a>

#### get

```python
def get(feature_name: str) -> FlagAdapterResponse
```

Get a feature flag.

<a id="integrations.feature_flag.harness.HarnessFeatureFlagAdapter.get_all_feature_names"></a>

#### get\_all\_feature\_names

```python
def get_all_feature_names() -> t.List[str]
```

Get all the feature flags.

<a id="integrations.feature_flag.harness.HarnessFeatureFlagAdapter.save"></a>

#### save

```python
def save(feature_name: str, flag: bool) -> None
```

Create a feature flag.

<a id="integrations.feature_flag.harness.HarnessFeatureFlagAdapter.save_many"></a>

#### save\_many

```python
def save_many(flags: t.Dict[str, bool]) -> None
```

Create many feature flags.

<a id="integrations.feature_flag.harness.HarnessFeatureFlagAdapter.delete"></a>

#### delete

```python
def delete(feature_name: str) -> None
```

Drop a feature flag.

<a id="integrations.feature_flag.harness.HarnessFeatureFlagAdapter.delete_many"></a>

#### delete\_many

```python
def delete_many(feature_names: t.List[str]) -> None
```

Drop many feature flags.

<a id="integrations.feature_flag.harness.HarnessFeatureFlagAdapter.apply_source"></a>

#### apply\_source

```python
def apply_source(source: DltSource, *namespace: str) -> DltSource
```

Apply the feature flags to a dlt source.

<a id="integrations.feature_flag.harness.HarnessFeatureFlagAdapter.__del__"></a>

#### \_\_del\_\_

```python
def __del__() -> None
```

Close the client.

<a id="integrations.feature_flag"></a>

# integrations.feature\_flag

Feature flag providers implement a uniform interface and are wrapped by an adapter.

The adapter is responsible for loading the correct provider and applying the feature flags within
various contexts in cdf. This allows for a clean separation of concerns and makes it easy to
implement new feature flag providers in the future.

<a id="integrations.feature_flag.ADAPTERS"></a>

#### ADAPTERS

Feature flag provider adapters classes by name.

<a id="integrations.feature_flag.get_feature_flag_adapter_cls"></a>

#### get\_feature\_flag\_adapter\_cls

```python
@with_config(sections=("feature_flags", ))
def get_feature_flag_adapter_cls(
    provider: str = dlt.config.value
) -> M.Result[t.Type[AbstractFeatureFlagAdapter], Exception]
```

Get a feature flag adapter by name.

**Arguments**:

- `provider` - The name of the feature flag adapter.
- `options` - The configuration for the feature flag adapter.
  

**Returns**:

  The feature flag adapter.

<a id="integrations.feature_flag.file"></a>

# integrations.feature\_flag.file

File-based feature flag provider.

<a id="integrations.feature_flag.file.FilesystemFeatureFlagAdapter"></a>

## FilesystemFeatureFlagAdapter Objects

```python
class FilesystemFeatureFlagAdapter(AbstractFeatureFlagAdapter)
```

A feature flag adapter that uses the filesystem.

<a id="integrations.feature_flag.file.FilesystemFeatureFlagAdapter.__init__"></a>

#### \_\_init\_\_

```python
@with_config(sections=("feature_flags", ))
def __init__(filesystem: fsspec.AbstractFileSystem,
             filename: str = dlt.config.value,
             **kwargs: t.Any) -> None
```

Initialize the filesystem feature flags.

**Arguments**:

- `filesystem` - The filesystem to use.
- `filename` - The filename to use for the feature flags.

<a id="integrations.feature_flag.file.FilesystemFeatureFlagAdapter.get"></a>

#### get

```python
def get(feature_name: str) -> FlagAdapterResponse
```

Get a feature flag.

**Arguments**:

- `feature_name` - The name of the feature flag.
  

**Returns**:

  The feature flag.

<a id="integrations.feature_flag.file.FilesystemFeatureFlagAdapter.get_all_feature_names"></a>

#### get\_all\_feature\_names

```python
def get_all_feature_names() -> t.List[str]
```

Get all feature flag names.

**Returns**:

  The feature flag names.

<a id="integrations.feature_flag.file.FilesystemFeatureFlagAdapter.save"></a>

#### save

```python
def save(feature_name: str, flag: bool) -> None
```

Save a feature flag.

**Arguments**:

- `feature_name` - The name of the feature flag.
- `flag` - The value of the feature flag.

<a id="integrations.feature_flag.file.FilesystemFeatureFlagAdapter.save_many"></a>

#### save\_many

```python
def save_many(flags: t.Dict[str, bool]) -> None
```

Save multiple feature flags.

**Arguments**:

- `flags` - The feature flags to save.

<a id="integrations.feature_flag.split"></a>

# integrations.feature\_flag.split

Split feature flag provider.

<a id="integrations.feature_flag.split.SplitFeatureFlagAdapter"></a>

## SplitFeatureFlagAdapter Objects

```python
class SplitFeatureFlagAdapter(AbstractFeatureFlagAdapter)
```

A feature flag adapter that uses Split.

<a id="integrations.feature_flag.split.SplitFeatureFlagAdapter.__init__"></a>

#### \_\_init\_\_

```python
def __init__(sdk_key: str, **kwargs: t.Any) -> None
```

Initialize the Split feature flags.

**Arguments**:

- `sdk_key` - The SDK key to use for Split.

<a id="integrations.feature_flag.noop"></a>

# integrations.feature\_flag.noop

No-op feature flag provider.

<a id="integrations.feature_flag.noop.NoopFeatureFlagAdapter"></a>

## NoopFeatureFlagAdapter Objects

```python
class NoopFeatureFlagAdapter(AbstractFeatureFlagAdapter)
```

A feature flag adapter that does nothing.

<a id="integrations.feature_flag.noop.NoopFeatureFlagAdapter.__init__"></a>

#### \_\_init\_\_

```python
def __init__(**kwargs: t.Any) -> None
```

Initialize the adapter.

<a id="integrations.feature_flag.base"></a>

# integrations.feature\_flag.base

<a id="integrations.feature_flag.base.FlagAdapterResponse"></a>

## FlagAdapterResponse Objects

```python
class FlagAdapterResponse(Enum)
```

Feature flag response.

This enum is used to represent the state of a feature flag. It is similar
to a boolean but with an extra state for when the flag is not found.

<a id="integrations.feature_flag.base.FlagAdapterResponse.ENABLED"></a>

#### ENABLED

The feature flag is enabled.

<a id="integrations.feature_flag.base.FlagAdapterResponse.DISABLED"></a>

#### DISABLED

The feature flag is disabled.

<a id="integrations.feature_flag.base.FlagAdapterResponse.NOT_FOUND"></a>

#### NOT\_FOUND

The feature flag is not found.

<a id="integrations.feature_flag.base.FlagAdapterResponse.__bool__"></a>

#### \_\_bool\_\_

```python
def __bool__() -> bool
```

Return True if the flag is enabled and False otherwise.

<a id="integrations.feature_flag.base.FlagAdapterResponse.__eq__"></a>

#### \_\_eq\_\_

```python
def __eq__(value: object) -> bool
```

Compare the flag to a boolean.

<a id="integrations.feature_flag.base.FlagAdapterResponse.from_bool"></a>

#### from\_bool

```python
@classmethod
def from_bool(cls, flag: bool) -> "FlagAdapterResponse"
```

Convert a boolean to a flag response.

<a id="integrations.feature_flag.base.AbstractFeatureFlagAdapter"></a>

## AbstractFeatureFlagAdapter Objects

```python
class AbstractFeatureFlagAdapter(abc.ABC)
```

Abstract feature flag adapter.

<a id="integrations.feature_flag.base.AbstractFeatureFlagAdapter.__init__"></a>

#### \_\_init\_\_

```python
def __init__(**kwargs: t.Any) -> None
```

Initialize the adapter.

<a id="integrations.feature_flag.base.AbstractFeatureFlagAdapter.get"></a>

#### get

```python
@abc.abstractmethod
def get(feature_name: str) -> FlagAdapterResponse
```

Get the feature flag.

<a id="integrations.feature_flag.base.AbstractFeatureFlagAdapter.__getitem__"></a>

#### \_\_getitem\_\_

```python
def __getitem__(feature_name: str) -> FlagAdapterResponse
```

Get the feature flag.

<a id="integrations.feature_flag.base.AbstractFeatureFlagAdapter.get_many"></a>

#### get\_many

```python
def get_many(feature_names: t.List[str]) -> t.Dict[str, FlagAdapterResponse]
```

Get many feature flags.

Implementations should override this method if they can optimize it. The default
will call get in a loop.

<a id="integrations.feature_flag.base.AbstractFeatureFlagAdapter.save"></a>

#### save

```python
@abc.abstractmethod
def save(feature_name: str, flag: bool) -> None
```

Save the feature flag.

<a id="integrations.feature_flag.base.AbstractFeatureFlagAdapter.__setitem__"></a>

#### \_\_setitem\_\_

```python
def __setitem__(feature_name: str, flag: bool) -> None
```

Save the feature flag.

<a id="integrations.feature_flag.base.AbstractFeatureFlagAdapter.save_many"></a>

#### save\_many

```python
def save_many(flags: t.Dict[str, bool]) -> None
```

Save many feature flags.

Implementations should override this method if they can optimize it. The default
will call save in a loop.

<a id="integrations.feature_flag.base.AbstractFeatureFlagAdapter.get_all_feature_names"></a>

#### get\_all\_feature\_names

```python
@abc.abstractmethod
def get_all_feature_names() -> t.List[str]
```

Get all feature names.

<a id="integrations.feature_flag.base.AbstractFeatureFlagAdapter.keys"></a>

#### keys

```python
def keys() -> t.List[str]
```

Get all feature names.

<a id="integrations.feature_flag.base.AbstractFeatureFlagAdapter.__iter__"></a>

#### \_\_iter\_\_

```python
def __iter__() -> t.Iterator[str]
```

Iterate over the feature names.

<a id="integrations.feature_flag.base.AbstractFeatureFlagAdapter.__contains__"></a>

#### \_\_contains\_\_

```python
def __contains__(feature_name: str) -> bool
```

Check if a feature flag exists.

<a id="integrations.feature_flag.base.AbstractFeatureFlagAdapter.__len__"></a>

#### \_\_len\_\_

```python
def __len__() -> int
```

Get the number of feature flags.

<a id="integrations.feature_flag.base.AbstractFeatureFlagAdapter.delete"></a>

#### delete

```python
def delete(feature_name: str) -> None
```

Delete a feature flag.

By default, this will disable the flag but implementations can override this method
to delete the flag.

<a id="integrations.feature_flag.base.AbstractFeatureFlagAdapter.delete_many"></a>

#### delete\_many

```python
def delete_many(feature_names: t.List[str]) -> None
```

Delete many feature flags.

<a id="integrations.feature_flag.base.AbstractFeatureFlagAdapter.apply_source"></a>

#### apply\_source

```python
def apply_source(source: "DltSource", *namespace: str) -> "DltSource"
```

Apply the feature flags to a dlt source.

**Arguments**:

- `source` - The source to apply the feature flags to.
  

**Returns**:

  The source with the feature flags applied.

