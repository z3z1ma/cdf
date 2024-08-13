"""Dependency registry with lifecycle management."""

import enum
import inspect
import logging
import sys
import types
import typing as t
from collections import ChainMap
from functools import partial, partialmethod, wraps

import pydantic
from typing_extensions import ParamSpec, Self

from cdf.core.injector.errors import DependencyCycleError

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
    """An instance dependency is a singleton that is already created."""

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
        return self.name == other[0] and _same_eff_type(self.type_, other[1])

    def __hash__(self) -> int:
        """Hash the key with the effective type if possible."""
        try:
            return hash((self.name, _get_eff_type(self.type_)))
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


def _is_annotation_typed(hint: t.Type) -> bool:
    """Check if a type hint constitutes a type we can use to resolve a dependency key.

    Args:
        hint: The type hint.

    Returns:
        True if the type hint is not a built-in type.
    """
    return hint not in (
        object,
        t.Any,
        None,
        type(None),
        t.NoReturn,
        inspect.Parameter.empty,
    )


def _get_eff_type(hint: t.Type) -> t.Type:
    """Get the effective type of a hint. This is the base type if it exists.

    Args:
        hint: The type hint.

    Returns:
        The effective type.
    """
    hint = _unwrap_optional(hint)
    if _is_union(hint):
        args = t.get_args(hint)
        if not args:
            return hint
        hint0 = _get_eff_type(args[0])
        if all(hint0 is _get_eff_type(arg) for arg in args[1:]):
            # Ex. Union[HarnessFFProvider, SplitFFProvider, LaunchDarklyFFProvider]
            # == BaseFFProvider
            return hint0
        return hint
    if not hasattr(hint, "__base__"):
        return hint
    if hint.__base__ in (None, object):
        return hint
    return hint.__base__


def _same_eff_type(hint1: t.Type, hint2: t.Type) -> bool:
    """Check if two type hints are of the same effective type.

    Args:
        hint1: The first type hint.
        hint2: The second type hint.

    Returns:
        True if the effective types are the same.
    """
    return _get_eff_type(hint1) is _get_eff_type(hint2)


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
    return TypedKey(k, _get_eff_type(t_))


def _safe_get_type_hints(obj: t.Any) -> t.Dict[str, t.Type]:
    """Get type hints for an object, ignoring errors.

    Args:
        obj: The object to get type hints for.

    Returns:
        A dictionary of attribute names to type hints.
    """
    try:
        return t.get_type_hints(obj)
    except Exception as e:
        logger.warning(f"Failed to get type hints for {obj!r}: {e}")
        return {}


class Dependency(pydantic.BaseModel, t.Generic[T], frozen=True):
    """An immutable dependency wrapper with lifecycle and initialization arguments."""

    factory: t.Union[t.Callable[..., T], T]
    """The factory or instance of the dependency."""

    lifecycle: Lifecycle = Lifecycle.SINGLETON
    """The lifecycle of the dependency."""

    @classmethod
    def instance(cls, instance: t.Any) -> "Dependency":
        """Create a dependency from an instance.

        Args:
            instance: The instance to use as the dependency.

        Returns:
            A new Dependency object with the instance lifecycle.
        """
        return cls(factory=instance, lifecycle=Lifecycle.INSTANCE)

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

    def apply(self, func: t.Callable[[T], T]) -> Self:
        """Apply a function to the unwrapped dependency.

        Args:
            func: The function to apply to the dependency.

        Returns:
            A new Dependency object with the function applied.
        """
        if self.lifecycle.is_instance:
            return self.__class__(factory=func(self.unwrap()), lifecycle=self.lifecycle)

        @wraps(func)
        def wrapper(*args: t.Any, **kwargs: t.Any) -> T:
            return func(self.unwrap(*args, **kwargs))

        return self.__class__(factory=wrapper, lifecycle=self.lifecycle)

    def apply_wrappers(
        self,
        *decorators: t.Callable[
            [t.Union[t.Callable[..., T], T]], t.Union[t.Callable[..., T], T]
        ],
    ) -> Self:
        """Apply decorators to the wrapped dependency factory.

        Args:
            decorators: The decorators to apply to the factory.

        Returns:
            A new Dependency object with the decorators applied.
        """
        factory = self.factory
        for decorator in decorators:
            factory = decorator(factory)
        return self.__class__(factory=factory, lifecycle=self.lifecycle)

    def __str__(self) -> str:
        return f"{self.factory} ({self.lifecycle})"

    def __repr__(self) -> str:
        return f"<Dependency {self!s}>"

    def unwrap(self, *args: t.Any, **kwargs: t.Any) -> T:
        """Unwrap the dependency."""
        if not callable(self.factory):
            return self.factory
        return self.factory(*args, **kwargs)

    __call__ = unwrap


class DependencyRegistry(t.MutableMapping):
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
        self._singletons: t.Dict[t.Union[str, TypedKey], t.Any] = {}
        self._resolving: t.Set[t.Union[str, TypedKey]] = set()

    @property
    def dependencies(self) -> ChainMap[t.Any, Dependency]:
        """Get all dependencies."""
        return ChainMap(self._typed_dependencies, self._untyped_dependencies)

    def add(
        self,
        name_or_key: DependencyKey,
        factory: t.Any,
        lifecycle: t.Optional[Lifecycle] = None,
        override: bool = False,
        init_args: t.Tuple[t.Any, ...] = (),
        init_kwargs: t.Optional[t.Dict[str, t.Any]] = None,
    ) -> None:
        """Register a dependency with the container.

        Args:
            name_or_key: The name or typed key of the dependency.
            factory: The factory or instance of the dependency.
            lifecycle: The lifecycle of the dependency.
            override: If True, override an existing dependency.
            init_args: Arguments to initialize the factory with.
            init_kwargs: Keyword arguments to initialize the factory with.
        """
        if isinstance(name_or_key, str):
            # Heuristic to infer the type of the dependency for more precise resolution
            # Classes are registered with their base type, functions with their return type
            if inspect.isclass(factory):
                key = TypedKey(name_or_key, _get_eff_type(factory))
            elif callable(factory):
                if hint := _safe_get_type_hints(factory).get("return"):
                    key = TypedKey(name_or_key, _get_eff_type(hint))
                else:
                    # In this case, the dependency is considered untyped
                    key = name_or_key
            else:
                # If the dependency is not a class or function
                # it is assumed to be an instance of a class
                key = TypedKey(name_or_key, _get_eff_type(type(factory)))

        key = _normalize_key(name_or_key)
        if self.has(key) and not override:
            raise ValueError(
                f'Dependency "{key}" is already registered, use a different name to avoid conflicts'
            )

        # Assume singleton lifecycle if the factory is callable
        if lifecycle is None:
            lifecycle = Lifecycle.SINGLETON if callable(factory) else Lifecycle.INSTANCE

        # If the factory lifecycle is deferred, it must be callable -- warn if not the case
        if lifecycle.is_deferred and not callable(factory):
            logger.warning(
                "Lifecycle is deferred but factory %s is not callable", factory
            )
            lifecycle = Lifecycle.INSTANCE

        # If the factory is callable and has initialization args, bind them early so
        # we don't need to schlepp them around
        if callable(factory) and (init_args or init_kwargs):
            factory = partial(factory, *init_args, **(init_kwargs or {}))

        # Register the dependency
        dep = Dependency(factory=factory, lifecycle=lifecycle)
        if isinstance(key, TypedKey):
            self._typed_dependencies[key] = dep
            # Allow untyped access to typed dependencies for convenience if not strict
            if not self.strict:
                self._untyped_dependencies[key.name] = dep
        else:
            self._untyped_dependencies[key] = dep

    add_prototype = partialmethod(add, lifecycle=Lifecycle.PROTOTYPE)
    add_singleton = partialmethod(add, lifecycle=Lifecycle.SINGLETON)
    add_instance = partialmethod(add, lifecycle=Lifecycle.INSTANCE)

    def add_from_dependency(
        self, name_or_key: DependencyKey, dependency: Dependency, override: bool = False
    ) -> None:
        """Add a Dependency object to the container.

        Args:
            name_or_key: The name or typed key of the dependency.
            dependency: The dependency object.
            override: If True, override an existing dependency
        """
        self.add(
            name_or_key,
            factory=dependency.factory,
            lifecycle=dependency.lifecycle,
            override=override,
        )

    def remove(self, name_or_key: DependencyKey) -> None:
        """Remove a dependency by name or key from the container.

        Args:
            name_or_key: The name or typed key of the dependency.
        """
        key = _normalize_key(name_or_key)
        if isinstance(key, str):
            if key in self._untyped_dependencies:
                del self._untyped_dependencies[key]
            raise KeyError(f'Dependency "{key}" is not registered')
        elif key in self._typed_dependencies:
            del self._typed_dependencies[key]
        else:
            raise KeyError(f'Dependency "{key}" is not registered')
        if key in self._singletons:
            del self._singletons[key]

    def clear(self) -> None:
        """Clear all dependencies and singletons."""
        self._typed_dependencies.clear()
        self._untyped_dependencies.clear()
        self._singletons.clear()

    def has(self, name_or_key: DependencyKey) -> bool:
        """Check if a dependency is registered.

        Args:
            name_or_key: The name or typed key of the dependency.
        """
        return name_or_key in self.dependencies

    def get(self, name_or_key: DependencyKey, must_exist: bool = False) -> t.Any:
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
                types = map(_get_eff_type, t.get_args(key.type_))
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
        if dep.lifecycle.is_prototype:
            self._resolving.add(key)
            try:
                return self.wire(dep.factory)()
            finally:
                self._resolving.remove(key)
        elif dep.lifecycle.is_singleton:
            if key not in self._singletons:
                self._resolving.add(key)
                try:
                    self._singletons[key] = self.wire(dep.factory)()
                finally:
                    self._resolving.remove(key)
            return self._singletons[key]
        elif dep.lifecycle.is_instance:
            if callable(dep.factory):
                return self.wire(dep.factory)
            return dep.factory

    get_or_raise = partialmethod(get, must_exist=True)

    def __contains__(self, key: t.Union[str, t.Tuple[str, t.Type]]) -> bool:
        """Check if a dependency is registered."""
        return self.has(key)

    def __getitem__(self, name: t.Union[str, t.Tuple[str, t.Type]]) -> t.Any:
        """Get a dependency. Raises KeyError if not found."""
        return self.get(name, must_exist=True)

    def __setitem__(self, name: str, factory: t.Any) -> None:
        """Add a dependency. Defaults to singleton lifecycle if callable, else instance."""
        self.add(name, factory, override=True)

    def __delitem__(self, name: str) -> None:
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
                    if _is_annotation_typed(param.annotation):
                        dep = self.get((name, param.annotation))
                    # Fallback to untyped injection
                    if dep is None:
                        dep = self.get(name)
                    # If a dependency is found, inject it
                    if dep is not None:
                        bound_args.arguments[name] = dep
            bound_args.apply_defaults()
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
        return self.wire(func_or_cls)(*args, **kwargs)

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

    def __add__(self, other: "DependencyRegistry") -> "DependencyRegistry":
        """Merge two registries."""
        self._untyped_dependencies = {
            **self._untyped_dependencies,
            **other._untyped_dependencies,
        }
        self._typed_dependencies = {
            **self._typed_dependencies,
            **other._typed_dependencies,
        }
        self._singletons = {
            **self._singletons,
            **other._singletons,
        }
        return self

    def __getstate__(self) -> t.Dict[str, t.Any]:
        """Serialize the state."""
        return {
            "_typed_dependencies": self._typed_dependencies,
            "_untyped_dependencies": self._untyped_dependencies,
            "_singletons": self._singletons,
            "_resolving": self._resolving,
        }

    def __setstate__(self, state: t.Dict[str, t.Any]) -> None:
        """Deserialize the state."""
        self._typed_dependencies = state["_typed_dependencies"]
        self._untyped_dependencies = state["_untyped_dependencies"]
        self._singletons = state["_singletons"]
        self._resolving = state["_resolving"]


GLOBAL_REGISTRY = DependencyRegistry()
"""A global dependency registry."""
