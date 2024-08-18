"""Dependency registry with lifecycle management."""

import enum
import inspect
import logging
import os
import sys
import types
import typing as t
from collections import ChainMap
from functools import partial, partialmethod, wraps

import pydantic
import pydantic_core
from typing_extensions import ParamSpec, Self

import cdf.core.configuration as conf
from cdf.core.injector.errors import DependencyCycleError, DependencyMutationError

logger = logging.getLogger(__name__)

T = t.TypeVar("T")
P = ParamSpec("P")

__all__ = [
    "DependencyRegistry",
    "Dependency",
    "Lifecycle",
    "DependencyKey",
    "GLOBAL_REGISTRY",
]


class Lifecycle(enum.Enum):
    """Lifecycle of a dependency."""

    PROTOTYPE = enum.auto()
    """A prototype dependency is created every time it is requested"""

    SINGLETON = enum.auto()
    """A singleton dependency is created once and shared."""

    INSTANCE = enum.auto()
    """An instance dependency is a global object which is not created by the container."""

    @property
    def is_prototype(self) -> bool:
        """Check if the lifecycle is prototype."""
        return self == Lifecycle.PROTOTYPE

    @property
    def is_singleton(self) -> bool:
        """Check if the lifecycle is singleton."""
        return self == Lifecycle.SINGLETON

    @property
    def is_instance(self) -> bool:
        """Check if the lifecycle is instance."""
        return self == Lifecycle.INSTANCE

    @property
    def is_deferred(self) -> bool:
        """Check if the object to be created is deferred."""
        return self.is_prototype or self.is_singleton

    def __str__(self) -> str:
        return self.name.lower()


class TypedKey(t.NamedTuple):
    """A key which is a tuple of a name and a type."""

    name: str
    type_: t.Type[t.Any]

    @property
    def type_name(self) -> t.Optional[str]:
        """Get the name of the type if applicable."""
        return self.type_.__name__

    def __str__(self) -> str:
        return f"{self.name}: {self.type_name}"

    def __repr__(self) -> str:
        return f"<TypedKey {self!s}>"

    def __eq__(self, other: t.Any) -> bool:
        """Two keys are equal if their names and base types match."""
        if not isinstance(other, (TypedKey, tuple)):
            return False
        return self.name == other[0] and _same_type(self.type_, other[1])

    def __hash__(self) -> int:
        """Hash the key with the effective type if possible."""
        try:
            return hash((self.name, _unwrap_type(self.type_)))
        except TypeError as e:
            logger.warning(f"Failed to hash key {self!r}: {e}")
            return hash((self.name, self.type_))


DependencyKey = t.Union[str, t.Tuple[str, t.Type[t.Any]], TypedKey]
"""A string or a typed key."""


def _unwrap_optional(hint: t.Type) -> t.Type:
    """Unwrap Optional type hint. Also unwraps types.UnionType like str | None

    Args:
        hint: The type hint.

    Returns:
        The unwrapped type hint.
    """
    args = t.get_args(hint)
    if len(args) != 2 or args[1] is not type(None):
        return hint
    return args[0]


def _is_union(hint: t.Type) -> bool:
    """Check if a type hint is a Union.

    Args:
        hint: The type hint.

    Returns:
        True if the type hint is a Union.
    """
    return hint is t.Union or (sys.version_info >= (3, 10) and hint is types.UnionType)


def _is_ambiguous_type(hint: t.Optional[t.Type]) -> bool:
    """Check if a type hint or Signature annotation is ambiguous.

    Args:
        hint: The type hint.

    Returns:
        True if the type hint is ambiguous.
    """
    return hint in (
        object,
        t.Any,
        None,
        type(None),
        t.NoReturn,
        inspect.Parameter.empty,
        type(lambda: None),
    )


def _unwrap_type(hint: t.Type) -> t.Type:
    """Unwrap a type hint.

    For a Union, this is the base type if all types are the same base type.
    Otherwise, it is the hint itself with Optional unwrapped.

    Args:
        hint: The type hint.

    Returns:
        The unwrapped type hint.
    """
    hint = _unwrap_optional(hint)
    if _is_union(hint):
        args = list(map(_unwrap_optional, t.get_args(hint)))
        if not args:
            return hint
        f_base = getattr(args[0], "__base__", None)
        if f_base and all(f_base is getattr(arg, "__base__", None) for arg in args[1:]):
            # Ex. Union[HarnessFFProvider, SplitFFProvider, LaunchDarklyFFProvider]
            # == BaseFFProvider
            return f_base
    return hint


def _same_type(hint1: t.Type, hint2: t.Type) -> bool:
    """Check if two type hints are of the same unwrapped type.

    Args:
        hint1: The first type hint.
        hint2: The second type hint.

    Returns:
        True if the unwrapped types are the same.
    """
    return _unwrap_type(hint1) is _unwrap_type(hint2)


@t.overload
def _normalize_key(key: str) -> str: ...


@t.overload
def _normalize_key(key: t.Union[t.Tuple[str, t.Any], TypedKey]) -> TypedKey: ...


def _normalize_key(
    key: t.Union[str, t.Tuple[str, t.Type[t.Any]], TypedKey],
) -> t.Union[str, TypedKey]:
    """Normalize a key 2-tuple to a TypedKey if it is not already, preserve str.

    Args:
        key: The key to normalize.

    Returns:
        The normalized key.
    """
    if isinstance(key, str):
        return key
    k, t_ = key
    return TypedKey(k, _unwrap_type(t_))


def _safe_get_type_hints(obj: t.Any) -> t.Dict[str, t.Type]:
    """Get type hints for an object, ignoring errors.

    Args:
        obj: The object to get type hints for.

    Returns:
        A dictionary of attribute names to type hints.
    """
    try:
        if isinstance(obj, partial):
            obj = obj.func
        return t.get_type_hints(obj)
    except Exception as e:
        logger.debug(f"Failed to get type hints for {obj!r}: {e}")
        return {}


class Dependency(pydantic.BaseModel, t.Generic[T]):
    """A Monadic type which wraps a value with lifecycle and allows simple transformations."""

    factory: t.Callable[..., T]
    """The factory or instance of the dependency."""
    lifecycle: Lifecycle = Lifecycle.SINGLETON
    """The lifecycle of the dependency."""

    conf_spec: t.Optional[t.Union[t.Tuple[str, ...], t.Dict[str, str]]] = None
    """A hint for configuration values."""

    _instance: t.Optional[T] = None
    """The instance of the dependency once resolved."""
    _is_resolved: bool = False
    """Flag to indicate if the dependency has been unwrapped."""

    @pydantic.model_validator(mode="after")
    def _apply_spec(self) -> Self:
        """Apply the configuration spec to the dependency."""
        spec = self.conf_spec
        if isinstance(spec, dict):
            self.map(conf.map_config_values(**spec))
        elif isinstance(spec, tuple):
            self.map(conf.map_config_section(*spec))
        return self

    @pydantic.model_validator(mode="before")
    @classmethod
    def _ensure_lifecycle(cls, data: t.Any) -> t.Any:
        """Ensure a valid lifecycle is set for the dependency."""
        from cdf.core.context import get_default_callable_lifecycle

        if isinstance(data, dict):
            factory = data["factory"]
            default_callable_lc = (
                get_default_callable_lifecycle() or Lifecycle.SINGLETON
            )
            lc = data.get(
                "lifecycle",
                default_callable_lc if callable(factory) else Lifecycle.INSTANCE,
            )
            if isinstance(lc, str):
                lc = Lifecycle[lc.upper()]
            if not isinstance(lc, Lifecycle):
                raise ValueError(f"Invalid lifecycle {lc=}")
            if not (lc.is_instance or callable(factory)):
                raise ValueError(f"Value must be callable for {lc=}")
            data["lifecycle"] = lc
        return data

    @pydantic.field_validator("factory", mode="before")
    @classmethod
    def _ensure_callable(cls, factory: t.Any) -> t.Any:
        """Ensure the factory is callable."""
        if not callable(factory):

            def defer() -> T:
                return factory

            defer.__name__ = f"factory_{os.urandom(4).hex()}"
            return defer
        return factory

    @classmethod
    def instance(cls, instance: t.Any) -> "Dependency":
        """Create a dependency from an instance.

        Args:
            instance: The instance to use as the dependency.

        Returns:
            A new Dependency object with the instance lifecycle.
        """
        obj = cls(factory=instance, lifecycle=Lifecycle.INSTANCE)
        obj._instance = instance
        obj._is_resolved = True
        return obj

    @classmethod
    def singleton(
        cls, factory: t.Callable[..., T], *args: t.Any, **kwargs: t.Any
    ) -> "Dependency":
        """Create a singleton dependency.

        Args:
            factory: The factory function to create the dependency.
            args: Positional arguments to pass to the factory.
            kwargs: Keyword arguments to pass to the factory.

        Returns:
            A new Dependency object with the singleton lifecycle.
        """
        if callable(factory) and (args or kwargs):
            factory = partial(factory, *args, **kwargs)
        return cls(factory=factory, lifecycle=Lifecycle.SINGLETON)

    @classmethod
    def prototype(
        cls, factory: t.Callable[..., T], *args: t.Any, **kwargs: t.Any
    ) -> "Dependency":
        """Create a prototype dependency.

        Args:
            factory: The factory function to create the dependency.
            args: Positional arguments to pass to the factory.
            kwargs: Keyword arguments to pass to the factory.

        Returns:
            A new Dependency object with the prototype lifecycle.
        """
        if callable(factory) and (args or kwargs):
            factory = partial(factory, *args, **kwargs)
        return cls(factory=factory, lifecycle=Lifecycle.PROTOTYPE)

    @classmethod
    def wrap(cls, obj: t.Any, *args: t.Any, **kwargs: t.Any) -> Self:
        """Wrap an object as a dependency.

        Assumes singleton lifecycle for callables unless a default lifecycle context is set.

        Args:
            obj: The object to wrap.

        Returns:
            A new Dependency object with the object as the factory.
        """
        if callable(obj):
            from cdf.core.context import get_default_callable_lifecycle

            if args or kwargs:
                obj = partial(obj, *args, **kwargs)
            default_callable_lc = (
                get_default_callable_lifecycle() or Lifecycle.SINGLETON
            )
            return cls(factory=obj, lifecycle=default_callable_lc)
        return cls(factory=obj, lifecycle=Lifecycle.INSTANCE)

    def map_value(self, func: t.Callable[[T], T]) -> Self:
        """Apply a function to the unwrapped value.

        Args:
            func: The function to apply to the unwrapped value.

        Returns:
            A new Dependency object with the function applied.
        """
        if self._is_resolved:
            self._instance = func(self._instance)  # type: ignore
            return self

        factory = self.factory

        @wraps(factory)
        def wrapper() -> T:
            return func(factory())

        self.factory = wrapper
        return self

    def map(
        self,
        *funcs: t.Callable[[t.Callable[..., T]], t.Callable[..., T]],
        idempotent: bool = False,
    ) -> Self:
        """Apply a sequence of transformations to the wrapped value.

        The transformations are applied in order. This is a no-op if the dependency is
        already resolved and idempotent is True or the dependency is an instance.

        Args:
            funcs: The functions to apply to the wrapped value.
            idempotent: If True, allow transformations on resolved dependencies to be a no-op.

        Returns:
             The Dependency object with the transformations applied.
        """
        if self._is_resolved:
            if self.lifecycle.is_instance or idempotent:
                return self
            raise DependencyMutationError(
                f"Dependency {self!r} is already resolved, cannot apply transformations to factory"
            )
        factory = self.factory
        for func in funcs:
            factory = func(factory)
        self.factory = factory
        return self

    def unwrap(self) -> T:
        """Unwrap the value from the factory."""
        if self.lifecycle.is_prototype:
            return self.factory()
        if self._instance is not None:
            return self._instance
        self._instance = self.factory()
        if self.lifecycle.is_singleton:
            self._is_resolved = True
        return self._instance

    def __str__(self) -> str:
        return f"{self.factory} ({self.lifecycle})"

    def __repr__(self) -> str:
        return f"<Dependency {self!s}>"

    def __call__(self) -> T:
        """Alias for unwrap."""
        return self.unwrap()

    def try_infer_type(self) -> t.Optional[t.Type[T]]:
        """Get the effective type of the dependency."""
        if inspect.isclass(self.factory):
            return _unwrap_type(self.factory)
        if inspect.isfunction(self.factory):
            if hint := _safe_get_type_hints(inspect.unwrap(self.factory)).get("return"):
                return _unwrap_type(hint)
        if self._is_resolved:
            return _unwrap_type(type(self._instance))

    def generate_key(self, name: DependencyKey) -> t.Union[str, TypedKey]:
        """Generate a typed key for the dependency.

        Args:
            name: The name of the dependency.

        Returns:
            A typed key if the type can be inferred, else the name.
        """
        if isinstance(name, TypedKey):
            return name
        elif isinstance(name, tuple):
            return TypedKey(name[0], name[1])
        hint = self.try_infer_type()
        return TypedKey(name, hint) if hint and not _is_ambiguous_type(hint) else name


class DependencyRegistry(t.MutableMapping[DependencyKey, Dependency]):
    """A registry for dependencies with lifecycle management.

    Dependencies can be registered with a name or a typed key. Typed keys are tuples
    of a name and a type hint. Dependencies can be added with a lifecycle, which can be
    one of prototype, singleton, or instance. Dependencies can be retrieved by name or
    typed key. Dependencies can be injected into functions or classes. Dependencies can
    be wired into callables to resolve a dependency graph.
    """

    lifecycle = Lifecycle

    def __init__(self, strict: bool = False) -> None:
        """Initialize the registry.

        Args:
            strict: If True, do not inject an untyped lookup for a typed dependency.
        """
        self.strict = strict
        self._typed_dependencies: t.Dict[TypedKey, Dependency] = {}
        self._untyped_dependencies: t.Dict[str, Dependency] = {}
        self._resolving: t.Set[t.Union[str, TypedKey]] = set()

    @property
    def dependencies(self) -> ChainMap[t.Any, Dependency]:
        """Get all dependencies."""
        return ChainMap(self._typed_dependencies, self._untyped_dependencies)

    def add(
        self,
        key: DependencyKey,
        value: t.Any,
        lifecycle: t.Optional[Lifecycle] = None,
        override: bool = False,
        init_args: t.Tuple[t.Any, ...] = (),
        init_kwargs: t.Optional[t.Dict[str, t.Any]] = None,
    ) -> None:
        """Register a dependency with the container.

        Args:
            key: The name of the dependency.
            value: The factory or instance of the dependency.
            lifecycle: The lifecycle of the dependency.
            override: If True, override an existing dependency.
            init_args: Arguments to initialize the factory with.
            init_kwargs: Keyword arguments to initialize the factory with.
        """

        # Assume singleton lifecycle if the value is callable unless set in context
        if lifecycle is None:
            from cdf.core.context import get_default_callable_lifecycle

            default_callable_lc = (
                get_default_callable_lifecycle() or Lifecycle.SINGLETON
            )
            lifecycle = default_callable_lc if callable(value) else Lifecycle.INSTANCE

        # If the value is callable and has initialization args, bind them early so
        # we don't need to schlepp them around
        if callable(value) and (init_args or init_kwargs):
            value = partial(value, *init_args, **(init_kwargs or {}))

        # Register the dependency
        dependency = Dependency(factory=value, lifecycle=lifecycle)
        dependency_key = dependency.generate_key(key)
        if self.has(dependency_key) and not override:
            raise ValueError(f'Dependency "{dependency_key}" is already registered')
        if isinstance(dependency_key, TypedKey):
            self._typed_dependencies[dependency_key] = dependency
            # Allow untyped access to typed dependencies for convenience if not strict
            # or if the hint is not a distinct type
            if not self.strict or _is_ambiguous_type(dependency_key.type_):
                self._untyped_dependencies[dependency_key.name] = dependency
        else:
            self._untyped_dependencies[dependency_key] = dependency

    add_prototype = partialmethod(add, lifecycle=Lifecycle.PROTOTYPE)
    add_singleton = partialmethod(add, lifecycle=Lifecycle.SINGLETON)
    add_instance = partialmethod(add, lifecycle=Lifecycle.INSTANCE)

    def add_from_dependency(
        self, key: DependencyKey, dependency: Dependency, override: bool = False
    ) -> None:
        """Add a Dependency object to the container.

        Args:
            key: The name or typed key of the dependency.
            dependency: The dependency object.
            override: If True, override an existing dependency
        """
        dependency_key = dependency.generate_key(key)
        if self.has(dependency_key) and not override:
            raise ValueError(
                f'Dependency "{dependency_key}" is already registered, use a different name to avoid conflicts'
            )
        if isinstance(dependency_key, TypedKey):
            self._typed_dependencies[dependency_key] = dependency
            if not self.strict or _is_ambiguous_type(dependency_key.type_):
                self._untyped_dependencies[dependency_key.name] = dependency
        else:
            self._untyped_dependencies[dependency_key] = dependency

    def remove(self, name_or_key: DependencyKey) -> None:
        """Remove a dependency by name or key from the container.

        Args:
            name_or_key: The name or typed key of the dependency.
        """
        key = _normalize_key(name_or_key)
        if isinstance(key, str):
            if key in self._untyped_dependencies:
                del self._untyped_dependencies[key]
            else:
                raise KeyError(f'Dependency "{key}" is not registered')
        elif key in self._typed_dependencies:
            del self._typed_dependencies[key]
        else:
            raise KeyError(f'Dependency "{key}" is not registered')

    def clear(self) -> None:
        """Clear all dependencies and singletons."""
        self._typed_dependencies.clear()
        self._untyped_dependencies.clear()

    def has(self, name_or_key: DependencyKey) -> bool:
        """Check if a dependency is registered.

        Args:
            name_or_key: The name or typed key of the dependency.
        """
        return name_or_key in self.dependencies

    def resolve(self, name_or_key: DependencyKey, must_exist: bool = False) -> t.Any:
        """Get a dependency.

        Args:
            name_or_key: The name or typed key of the dependency.
            must_exist: If True, raise KeyError if the dependency is not found.

        Returns:
            The dependency if found, else None.
        """
        key = _normalize_key(name_or_key)

        # Resolve the dependency
        if isinstance(key, str):
            if key not in self._untyped_dependencies:
                if must_exist:
                    raise KeyError(f'Dependency "{key}" is not registered')
                return
            dep = self.dependencies[key]
        else:
            if _is_union(key.type_):
                types = map(_unwrap_type, t.get_args(key.type_))
            else:
                types = [key.type_]
            for type_ in types:
                key = TypedKey(key.name, type_)
                if self.has(key):
                    break
            else:
                if must_exist:
                    raise KeyError(f'Dependency "{key}" is not registered')
                return
            dep = self.dependencies[key]

        # Detect dependency cycles
        if key in self._resolving:
            raise DependencyCycleError(
                f"Dependency cycle detected while resolving {key} for {dep.factory!r}"
            )

        # Handle the lifecycle of the dependency, recursively resolving dependencies
        self._resolving.add(key)
        try:
            return dep.map(self.wire).unwrap()
        except DependencyMutationError:
            return dep.unwrap()
        finally:
            self._resolving.remove(key)

    resolve_or_raise = partialmethod(resolve, must_exist=True)

    def __contains__(self, name: t.Any) -> bool:
        """Check if a dependency is registered."""
        return self.has(name)

    def __getitem__(self, name: DependencyKey) -> t.Any:
        """Get a dependency. Raises KeyError if not found."""
        return self.resolve(name, must_exist=True)

    def __setitem__(self, name: DependencyKey, value: t.Any) -> None:
        """Add a dependency. Defaults to singleton lifecycle if callable, else instance."""
        self.add(name, value, override=True)

    def __delitem__(self, name: DependencyKey) -> None:
        """Remove a dependency."""
        self.remove(name)

    def wire(self, func_or_cls: t.Callable[P, T]) -> t.Callable[..., T]:
        """Inject dependencies into a function.

        Args:
            func_or_cls: The function or class to inject dependencies into.

        Returns:
            A function that can be called with dependencies injected
        """
        if not callable(func_or_cls):
            return func_or_cls

        sig = inspect.signature(func_or_cls)

        @wraps(func_or_cls)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
            bound_args = sig.bind_partial(*args, **kwargs)
            for name, param in sig.parameters.items():
                if param.kind in (param.VAR_POSITIONAL, param.VAR_KEYWORD):
                    continue
                if param.default not in (param.empty, None):
                    continue
                if name not in bound_args.arguments:
                    dep = None
                    # Try to resolve a typed dependency
                    if not _is_ambiguous_type(param.annotation):
                        dep = self.resolve((name, param.annotation))
                    # Fallback to untyped injection
                    if dep is None:
                        dep = self.resolve(name)
                    # If a dependency is found, inject it
                    if dep is not None:
                        bound_args.arguments[name] = dep
            return func_or_cls(*bound_args.args, **bound_args.kwargs)

        return wrapper

    def __call__(
        self, func_or_cls: t.Callable[P, T], *args: t.Any, **kwargs: t.Any
    ) -> T:
        """Invoke a callable with dependencies injected from the registry.

        Args:
            func_or_cls: The function or class to invoke.
            args: Positional arguments to pass to the callable.
            kwargs: Keyword arguments to pass to the callable.

        Returns:
            The result of the callable
        """
        wired_f = self.wire(func_or_cls)
        if not callable(wired_f):
            return wired_f
        return wired_f(*args, **kwargs)

    def __iter__(self) -> t.Iterator[TypedKey]:
        """Iterate over dependency names."""
        return iter(self.dependencies)

    def __len__(self) -> int:
        """Return the number of dependencies."""
        return len(self.dependencies)

    def __repr__(self) -> str:
        return f"<DependencyRegistry {self.dependencies.keys()}>"

    def __str__(self) -> str:
        return repr(self)

    def __bool__(self) -> bool:
        """True if the registry has dependencies."""
        return bool(self.dependencies)

    def __or__(self, other: "DependencyRegistry") -> "DependencyRegistry":
        """Merge two registries like pythons dict union overload."""
        self._untyped_dependencies = {
            **self._untyped_dependencies,
            **other._untyped_dependencies,
        }
        self._typed_dependencies = {
            **self._typed_dependencies,
            **other._typed_dependencies,
        }
        return self

    def __getstate__(self) -> t.Dict[str, t.Any]:
        """Serialize the state."""
        return {
            "_typed_dependencies": self._typed_dependencies,
            "_untyped_dependencies": self._untyped_dependencies,
            "_resolving": self._resolving,
        }

    def __setstate__(self, state: t.Dict[str, t.Any]) -> None:
        """Deserialize the state."""
        self._typed_dependencies = state["_typed_dependencies"]
        self._untyped_dependencies = state["_untyped_dependencies"]
        self._resolving = state["_resolving"]

    @classmethod
    def __get_pydantic_core_schema__(
        cls, source_type: t.Any, handler: pydantic.GetCoreSchemaHandler
    ) -> pydantic_core.CoreSchema:
        return pydantic_core.core_schema.dict_schema(
            keys_schema=pydantic_core.core_schema.union_schema(
                [
                    pydantic_core.core_schema.str_schema(),
                    pydantic_core.core_schema.tuple_schema(
                        [
                            pydantic_core.core_schema.str_schema(),
                            pydantic_core.core_schema.any_schema(),
                        ]
                    ),
                    pydantic_core.core_schema.is_instance_schema(TypedKey),
                ]
            ),
            values_schema=pydantic_core.core_schema.any_schema(),
        )


GLOBAL_REGISTRY = DependencyRegistry()
"""A global dependency registry."""
