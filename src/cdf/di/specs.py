"""cdf.di specs.

NB: The cdf.di.{Object,Singleton,...} functions follow the same
pattern as dataclasses.field() vs dataclasses.Field:
in order for typing to work for the user, we have dummy functions
that mimic expected typing behavior.
"""

from __future__ import annotations

import typing as t

from typing_extensions import ParamSpec, TypeAlias, override

from . import errors as di_errors

MISSING = object()
MISSING_DICT: dict = dict()  # Need a special typed sentinel for mypy

SpecID: TypeAlias = int
P = ParamSpec("P")
T = t.TypeVar("T")


def instantiate(cls: type[T], *args: t.Any, **kwargs: t.Any) -> T:
    """Instantiate obj from Spec parts."""
    try:
        if issubclass(
            cls,
            (PrototypeMixin, SingletonMixin),
        ):
            obj = cls.__new__(cls, _materialize=True)
            obj.__init__(*args, **kwargs)  # noqa
            return t.cast(T, obj)

        return cls(*args, **kwargs)
    except TypeError as exc:
        raise TypeError(f"{cls}: {str(exc)}") from None


class AttrFuture:
    """Future representing attr access on a Spec by its spec id."""

    def __init__(self, root_spec_id: SpecID, attrs: list[str]) -> None:
        self.root_spec_id = root_spec_id
        self.attrs = attrs

    def __getattr__(self, attr: str) -> AttrFuture:
        return AttrFuture(self.root_spec_id, self.attrs + [attr])


class Spec(t.Generic[T]):
    """Represents delayed object to be instantiated later."""

    _INTERNAL_FIELDS = ["spec_id"]
    NEXT_SPEC_ID = 0

    def __init__(self, spec_id: SpecID | None = None) -> None:
        self.spec_id = self._get_next_spec_id() if spec_id is None else spec_id

    def __getattr__(self, attr: str) -> AttrFuture:
        return AttrFuture(self.spec_id, [attr])

    @override
    def __setattr__(self, name: str, value: t.Any) -> None:
        if (
            name.startswith("__")
            or name == "_INTERNAL_FIELDS"
            or name in self._INTERNAL_FIELDS
        ):
            return super().__setattr__(name, value)

        # NB: We considered supporting this kind of perturbation,
        # but the issue is that we don't know whether the config
        # this spec is attached to has been frozen. For sake of safety
        # and simplicity, we raise an error here instead.
        raise di_errors.PerturbSpecError(
            "Cannot set on a spec. "
            "If you'd like to perturb a value used by a spec, "
            "promote it to be a config field and perturb the config instead."
        )

    # For mypy
    def __call__(self, *args: t.Any, **kwargs: t.Any) -> t.Any:
        return None

    @classmethod
    def _get_next_spec_id(cls) -> SpecID:
        # NB: Need to use Spec explicitly to ensure all Spec
        # subclasses share the same spec id space.
        result = Spec.NEXT_SPEC_ID
        Spec.NEXT_SPEC_ID += 1
        return result


class _Object(Spec[T]):
    """Represents fully-instantiated object to pass through."""

    _INTERNAL_FIELDS = Spec._INTERNAL_FIELDS + ["obj"]

    def __init__(self, obj: T, spec_id: SpecID | None = None) -> None:
        super().__init__(spec_id=spec_id)
        self.obj = obj


def Object(obj: T) -> T:  # noqa: N802
    """Spec to pass through a fully-instantiated object.

    Args:
        obj: Fully-instantiated object to pass through.
    """
    # Cast because the return type will act like a T
    return t.cast(T, _Object(obj))


class _Input(Spec[T]):
    """Represents user input to config."""

    _INTERNAL_FIELDS = Spec._INTERNAL_FIELDS + ["type_", "default"]

    def __init__(self, type_: type[T] | None = None, default: t.Any = MISSING) -> None:
        super().__init__()
        self.type_ = type_
        self.default = default


class _GlobalInput(_Input[T]):
    """Represents input passed in at config instantiation."""

    pass


def GlobalInput(  # noqa: N802
    type_: type[T] | None = None, default: t.Any = MISSING
) -> T:
    """Spec to use user input passed in at config instantiation.

    Args:
        type_: Expected type of input, for both static and runtime check.
        default: Default value if no input is provided.
    """
    # Cast because the return type will act like a T
    return t.cast(T, _GlobalInput(type_=type_, default=default))


class _LocalInput(_Input[T]):
    """Represents input passed in at config declaration."""

    pass


def LocalInput(  # noqa: N802
    type_: type[T] | None = None, default: t.Any = MISSING
) -> T:
    """Spec to use user input passed in at config declaration.

    Args:
        type_: Expected type of input, for both static and runtime check.
        default: Default value if no input is provided.
    """
    # Cast because the return type will act like a T
    return t.cast(T, _LocalInput(type_=type_, default=default))


class _Callable(Spec[T]):
    """Represents callable (e.g., func, type) to be called with given args."""

    _INTERNAL_FIELDS = Spec._INTERNAL_FIELDS + [
        "func_or_type",
        "args",
        "lazy_kwargs",
        "kwargs",
    ]

    def __init__(
        self,
        func_or_type: t.Callable[..., T],
        *args: t.Any,
        **kwargs: t.Any,
    ) -> None:
        super().__init__()
        self.func_or_type = func_or_type
        self.args = args
        self.lazy_kwargs = kwargs.pop("__lazy_kwargs", None)
        self.kwargs = kwargs

    def instantiate(self) -> t.Any:
        """Instantiate spec into object."""
        if isinstance(self.func_or_type, type):
            return instantiate(self.func_or_type, *self.args, **self.kwargs)
        else:
            # Non-type callable (e.g., function, functor)
            return self.func_or_type(*self.args, **self.kwargs)

    def copy_with(self, *args: t.Any, **kwargs: t.Any) -> _Callable:
        """Make a copy with replaced args.

        Used to replace arg specs with materialized args.
        """
        return self.__class__(self.func_or_type, *args, **kwargs)


class _Prototype(_Callable[T]):
    pass


def Prototype(  # noqa: N802
    func_or_type: t.Callable[P, T], *args: P.args, **kwargs: P.kwargs
) -> T:
    """Spec to call with args and no caching."""
    # Cast because the return type will act like a T
    return t.cast(T, _Prototype(func_or_type, *args, **kwargs))


def _identity(obj: T) -> T:
    return obj


def _union_dict_and_kwargs(values: dict, **kwargs: t.Any) -> dict:
    new_values = values.copy()
    new_values.update(**kwargs)
    return new_values


def Forward(obj: T) -> T:  # noqa: N802
    """Spec to simply forward to other spec."""
    # Cast because the return type will act like a T
    return t.cast(T, _Prototype(_identity, obj))


class _Singleton(_Callable[T]):
    pass


def Singleton(  # noqa: N802
    func_or_type: t.Callable[P, T], *args: P.args, **kwargs: P.kwargs
) -> T:
    """Spec to call with args and caching per config field."""
    # Cast because the return type will act like a T
    return t.cast(T, _Singleton(func_or_type, *args, **kwargs))


def SingletonTuple(*args: T) -> tuple[T]:  # noqa: N802
    """Spec to create tuple with args and caching per config field."""
    # Cast because the return type will act like a tuple of T
    return t.cast("tuple[T]", _Singleton(tuple, args))


def SingletonList(*args: T) -> list[T]:  # noqa: N802
    """Spec to create list with args and caching per config field."""
    # Cast because the return type will act like a list of T
    return t.cast("list[T]", _Singleton(list, args))


def SingletonDict(  # noqa: N802
    values: dict[t.Any, T] = MISSING_DICT,  # noqa
    /,
    **kwargs: T,
) -> dict[t.Any, T]:
    """Spec to create dict with args and caching per config field.

    Can specify either by pointing to a dict, passing in kwargs,
    or unioning both.

    >>> import cdf.di
    >>> spec0 = cdf.di.Object(1); spec1 = cdf.di.Object(2)
    >>> cdf.di.SingletonDict({"x": spec0, "y": spec1}) is not None
    True

    Or, alternatively:

    >>> cdf.di.SingletonDict(x=spec0, y=spec1) is not None
    True
    """
    if values is MISSING_DICT:
        # Cast because the return type will act like a dict of T
        return t.cast("dict[t.Any, T]", _Singleton(dict, **kwargs))
    else:
        # Cast because the return type will act like a dict of T
        return t.cast(
            "dict[t.Any, T]",
            _Singleton(_union_dict_and_kwargs, values, **kwargs),
        )


class PrototypeMixin:
    """Helper class for Prototype to ease syntax in Config.

    Equivalent to cdf.di.Prototype(cls, ...).
    """

    def __new__(
        cls: type, *args: t.Any, _materialize: bool = False, **kwargs: t.Any
    ) -> t.Any:
        if _materialize:
            return super().__new__(cls)  # type: ignore[misc]
        else:
            return Prototype(cls, *args, **kwargs)


class SingletonMixin:
    """Helper class for Singleton to ease syntax in Config.

    Equivalent to cdf.di.Singleton(cls, ...).
    """

    def __new__(
        cls: type, *args: t.Any, _materialize: bool = False, **kwargs: t.Any
    ) -> t.Any:
        if _materialize:
            return super().__new__(cls)  # type: ignore[misc]
        else:
            return Singleton(cls, *args, **kwargs)
