"""Typed resource decorator and yielded value contracts."""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping, Sequence
from typing import ParamSpec, Protocol, TypeVar, overload, runtime_checkable

JsonScalar = str | int | float | bool | None
JsonValue = JsonScalar | Mapping[str, "JsonValue"] | Sequence["JsonValue"]
Row = Mapping[str, JsonValue]


@runtime_checkable
class ArrowArrayExport(Protocol):
    def __arrow_c_array__(
        self, requested_schema: object | None = None, /
    ) -> tuple[object, object]: ...


@runtime_checkable
class ArrowStreamExport(Protocol):
    def __arrow_c_stream__(self, requested_schema: object | None = None, /) -> object: ...


ResourceYield = Row | ArrowArrayExport | ArrowStreamExport

P = ParamSpec("P")
R = TypeVar("R", bound=Callable[..., Iterable[ResourceYield]])


@overload
def resource(func: R, /) -> R: ...


@overload
def resource(
    *,
    name: str | None = None,
    primary_key: Sequence[str] = (),
    merge_key: Sequence[str] = (),
    cursor: str | None = None,
    parallel: bool = False,
    bounded: bool = True,
    schema: Mapping[str, str | tuple[str, bool]] | None = None,
    write_disposition: str = "append",
) -> Callable[[R], R]: ...


def resource(
    func: R | None = None,
    /,
    *,
    name: str | None = None,
    primary_key: Sequence[str] = (),
    merge_key: Sequence[str] = (),
    cursor: str | None = None,
    parallel: bool = False,
    bounded: bool = True,
    schema: Mapping[str, str | tuple[str, bool]] | None = None,
    write_disposition: str = "append",
) -> R | Callable[[R], R]:
    def decorate(inner: R) -> R:
        setattr(inner, "__cdf_resource__", True)
        setattr(inner, "__cdf_name__", name)
        setattr(inner, "__cdf_primary_key__", tuple(primary_key))
        setattr(inner, "__cdf_merge_key__", tuple(merge_key))
        setattr(inner, "__cdf_cursor__", cursor)
        setattr(inner, "__cdf_parallel__", parallel)
        setattr(inner, "__cdf_bounded__", bounded)
        setattr(
            inner,
            "__cdf_schema__",
            tuple(
                (field, value, True)
                if isinstance(value, str)
                else (field, value[0], value[1])
                for field, value in (schema or {}).items()
            ),
        )
        setattr(inner, "__cdf_write_disposition__", write_disposition)
        return inner

    if func is not None:
        return decorate(func)
    return decorate
