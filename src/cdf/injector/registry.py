import inspect
import types
import typing as t
from collections import ChainMap
from enum import Enum, auto
from functools import wraps

from typing_extensions import ParamSpec

T = t.TypeVar("T")
P = ParamSpec("P")


class Lifecycle(Enum):
    """Lifecycle of a dependency."""

    PROTOTYPE = auto()
    SINGLETON = auto()
    INSTANCE = auto()

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


class DependencyCycleError(Exception):
    """Raised when a dependency cycle is detected."""

    pass


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
        return f"<DependencyKey {self}>"

    def __eq__(self, other: t.Any) -> bool:
        """Two keys are equal if their names and base types match.

        If the key is untyped, only the name is compared with the other key.
        """
        if not isinstance(other, (TypedKey, tuple)):
            return False
        return self.name == other[0] and _same_effective_type(self.type_, other[1])

    def __hash__(self) -> int:
        return hash((self.name, _get_effective_type(self.type_)))


StringOrKey = t.Union[str, t.Tuple[str, t.Type[t.Any]], TypedKey]
"""A string or a typed key."""


def _unwrap_optional(hint: t.Type) -> t.Type:
    """Unwrap Optional type hint. Also unwraps types.UnionType like str | None"""
    args = t.get_args(hint)
    if len(args) != 2 or args[1] is not type(None):
        return hint
    return args[0]


def _is_union(hint: t.Type) -> bool:
    """Check if a type hint is a Union."""
    return hint is t.Union or type(hint) is types.UnionType


def _is_typed(hint: t.Type) -> bool:
    """Check if a type hint constitutes a type"""
    return hint in (
        object,
        t.Any,
        None,
        type(None),
        t.NoReturn,
        inspect.Parameter.empty,
    )


def _get_effective_type(hint: t.Type) -> t.Type:
    """Get the effective type of a hint. This is the base type if it exists."""
    hint = _unwrap_optional(hint)
    if _is_union(hint):
        args = t.get_args(hint)
        if not args:
            return object
        hint0 = _get_effective_type(args[0])
        if all(hint0 is _get_effective_type(arg) for arg in args[1:]):
            return hint0
        return hint
    if not hasattr(hint, "__base__"):
        return hint
    if hint.__base__ is None:
        return hint
    if hint.__base__ is object:
        return hint
    return hint.__base__


def _same_effective_type(hint1: t.Type, hint2: t.Type) -> bool:
    """Check if two type hints are of the same effective type."""
    return _get_effective_type(hint1) is _get_effective_type(hint2)


@t.overload
def _normalize_key(key: str) -> str: ...


@t.overload
def _normalize_key(key: t.Union[t.Tuple[str, t.Any], TypedKey]) -> TypedKey: ...


def _normalize_key(
    key: t.Union[str, t.Tuple[str, t.Type[t.Any]], TypedKey],
) -> t.Union[str, TypedKey]:
    """Normalize a key 2-tuple to a TypedKey if it is not already, preserve str."""
    if isinstance(key, str):
        return key
    k, t_ = key
    return TypedKey(k, _get_effective_type(t_))


class Dependency(t.NamedTuple):
    """A dependency with lifecycle and initialization arguments."""

    dependency: t.Any
    lifecycle: Lifecycle
    init_args: t.Tuple[t.Tuple[t.Any, ...], t.Dict[str, t.Any]]


class DependencyRegistry:
    lifecycle = Lifecycle

    def __init__(self) -> None:
        """Initialize the registry."""
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
        name_or_key: StringOrKey,
        dependency: t.Any,
        lifecycle: t.Optional[Lifecycle] = None,
        override: bool = False,
        *init_args: t.Any,
        **init_kwargs: t.Any,
    ) -> None:
        """Register a dependency with the container."""
        if isinstance(name_or_key, str):
            # Heuristic to infer the type of the dependency for more precise resolution
            # Classes are registered with their base type, functions with their return type
            if inspect.isclass(dependency):
                key = TypedKey(name_or_key, _get_effective_type(dependency))
            elif callable(dependency):
                if hint := t.get_type_hints(dependency).get("return"):
                    key = TypedKey(name_or_key, _get_effective_type(hint))
                else:
                    # In this case, the dependency is considered untyped
                    key = name_or_key
            else:
                # If the dependency is not a class or function
                # it is assumed to be an instance of a class
                key = TypedKey(name_or_key, _get_effective_type(type(dependency)))
        key = _normalize_key(name_or_key)
        if self.has(key) and not override:
            raise ValueError(
                f'Dependency "{key}" is already registered, use a different name to avoid conflicts'
            )
        if lifecycle is None:
            lifecycle = (
                Lifecycle.SINGLETON if callable(dependency) else Lifecycle.INSTANCE
            )
        if lifecycle.is_deferred and not callable(dependency):
            raise ValueError("Prototype and singleton dependencies must be callable")
        dep = Dependency(dependency, lifecycle, (init_args, init_kwargs))
        if isinstance(key, TypedKey):
            self._typed_dependencies[key] = dep
            key = key.name
        # Store a reference to the dependency by name for untyped injection
        self._untyped_dependencies[key] = dep

    def remove(self, name_or_key: StringOrKey) -> None:
        """Remove a dependency by name or key from the container."""
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

    def has(self, name_or_key: StringOrKey) -> bool:
        """Check if a dependency is registered."""
        return name_or_key in self.dependencies

    def get(self, name_or_key: StringOrKey, must_exist: bool = False) -> t.Any:
        """Get a dependency"""
        key = _normalize_key(name_or_key)
        if isinstance(key, str):
            if key not in self._untyped_dependencies:
                if must_exist:
                    raise KeyError(f'Dependency "{key}" is not registered')
                return
            # TODO: we should warn on untyped access since it's not a best practice, though it's supported
            dependency, lifecycle, (args, kwargs) = self.dependencies[key]
        else:
            if _is_union(key.type_):
                types = map(_get_effective_type, t.get_args(key.type_))
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
            dependency, lifecycle, (args, kwargs) = self.dependencies[key]

        if key in self._resolving:
            raise DependencyCycleError(f"Dependency cycle detected: {key}")

        if lifecycle.is_prototype:
            self._resolving.add(key)
            try:
                return dependency(*args, **kwargs)
            finally:
                self._resolving.remove(key)
        elif lifecycle.is_singleton:
            if key not in self._singletons:
                self._resolving.add(key)
                try:
                    self._singletons[key] = dependency(*args, **kwargs)
                finally:
                    self._resolving.remove(key)
            return self._singletons[key]
        elif lifecycle.is_instance:
            return dependency
        else:
            raise ValueError(f"Unknown lifecycle: {lifecycle}")

    def __contains__(self, key: t.Union[str, t.Tuple[str, t.Type]]) -> bool:
        """Check if a dependency is registered."""
        return self.has(key)

    def __getitem__(self, name: t.Union[str, t.Tuple[str, t.Type]]) -> t.Any:
        """Get a dependency. Raises KeyError if not found."""
        return self.get(name, must_exist=True)

    def __setitem__(self, name: str, dependency: t.Any) -> None:
        """Add a dependency. Defaults to singleton lifecycle if callable, else instance."""
        self.add(name, dependency)

    def __delitem__(self, name: str) -> None:
        """Remove a dependency."""
        self.remove(name)

    def inject_defaults(self, func_or_cls: t.Callable[P, T]) -> t.Callable[P, T]:
        """Inject dependencies into a function."""
        _instance = inspect.unwrap(func_or_cls)
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
                    obj = None
                    # Try to resolve a typed dependency
                    if _is_typed(param.annotation):
                        obj = self.get((name, param.annotation))
                    # Fallback to untyped injection
                    if obj is None:
                        obj = self.get(name)
                    # If a dependency is found, inject it
                    if obj is not None:
                        bound_args.arguments[name] = obj
            return func_or_cls(*bound_args.args, **bound_args.kwargs)

        return wrapper

    def wire(self, func_or_cls: t.Callable[P, T]) -> t.Callable[..., T]:
        """Wire dependencies into a callable recursively resolving the graph."""
        if not callable(func_or_cls):
            raise ValueError("Argument must be a callable")

        def recursive_inject(func: t.Callable[P, T]) -> t.Callable[P, T]:
            sig = inspect.signature(func)
            for name, param in sig.parameters.items():
                if param.kind in (param.VAR_POSITIONAL, param.VAR_KEYWORD):
                    continue
                hint = param.annotation
                if _is_typed(hint):
                    candidate = _normalize_key((name, hint))
                else:
                    candidate = name
                if not self.has(candidate):
                    continue
                dependency, lifecycle, (args, kwargs) = self.dependencies[candidate]
                if callable(dependency):
                    dependency = recursive_inject(dependency)
                self.add(
                    candidate, dependency, lifecycle, override=True, *args, **kwargs
                )
            return self.inject_defaults(func)

        return recursive_inject(func_or_cls)

    def __call__(
        self, func_or_cls: t.Callable[P, T], *args: t.Any, **kwargs: t.Any
    ) -> T:
        """Invoke a callable with dependencies injected from the registry."""
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


GLOBAL_REGISTRY = DependencyRegistry()

__all__ = ["DependencyRegistry", "Lifecycle", "DependencyCycleError", "GLOBAL_REGISTRY"]
