"""Preview dlt-compatible authoring shims for CDF fixtures."""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping, MutableMapping, Sequence
from dataclasses import dataclass
from typing import Literal, ParamSpec, TypeVar, overload

from .resource import JsonValue, ResourceYield

P = ParamSpec("P")
R = TypeVar("R", bound=Callable[..., Iterable[ResourceYield]])
S = TypeVar("S", bound=Callable[..., object])

DltWriteDisposition = Literal["append", "replace", "merge", "skip"]
DltOrdering = Literal["exact", "inexact", "unordered"]


@dataclass(frozen=True, slots=True)
class Incremental:
    cursor_path: str
    initial_value: JsonValue | None = None
    end_value: JsonValue | None = None
    row_order: Literal["asc", "desc"] | None = None
    ordering: DltOrdering = "inexact"
    lag_tolerance_ms: int = 0

    def to_metadata(self) -> dict[str, object]:
        return {
            "cursor_path": self.cursor_path,
            "initial_value": self.initial_value,
            "end_value": self.end_value,
            "row_order": self.row_order,
            "ordering": self.ordering,
            "lag_tolerance_ms": self.lag_tolerance_ms,
        }


class _Current:
    def __init__(self) -> None:
        self._resource_state: dict[str, JsonValue] = {}
        self._source_state: dict[str, JsonValue] = {}

    @property
    def state(self) -> MutableMapping[str, JsonValue]:
        return self.resource_state()

    def resource_state(
        self, resource_name: str | None = None
    ) -> MutableMapping[str, JsonValue]:
        _ = resource_name
        return self._resource_state

    def source_state(
        self, source_state_key: str | None = None
    ) -> MutableMapping[str, JsonValue]:
        _ = source_state_key
        return self._source_state


current = _Current()


def incremental(
    cursor_path: str,
    *,
    initial_value: JsonValue | None = None,
    end_value: JsonValue | None = None,
    row_order: Literal["asc", "desc"] | None = None,
    ordering: DltOrdering = "inexact",
    lag_tolerance_ms: int = 0,
) -> Incremental:
    return Incremental(
        cursor_path=cursor_path,
        initial_value=initial_value,
        end_value=end_value,
        row_order=row_order,
        ordering=ordering,
        lag_tolerance_ms=lag_tolerance_ms,
    )


@overload
def resource(func: R, /) -> R: ...


@overload
def resource(
    *,
    name: str | None = None,
    table_name: str | None = None,
    primary_key: str | Sequence[str] | None = None,
    merge_key: str | Sequence[str] | None = None,
    write_disposition: DltWriteDisposition | Mapping[str, object] | None = None,
    schema_contract: str | Mapping[str, str] | None = None,
    selected: bool = True,
    parallelized: bool = False,
    incremental: Incremental | None = None,
) -> Callable[[R], R]: ...


def resource(
    func: R | None = None,
    /,
    *,
    name: str | None = None,
    table_name: str | None = None,
    primary_key: str | Sequence[str] | None = None,
    merge_key: str | Sequence[str] | None = None,
    write_disposition: DltWriteDisposition | Mapping[str, object] | None = None,
    schema_contract: str | Mapping[str, str] | None = None,
    selected: bool = True,
    parallelized: bool = False,
    incremental: Incremental | None = None,
) -> R | Callable[[R], R]:
    def decorate(inner: R) -> R:
        metadata: dict[str, object] = {
            "kind": "resource",
            "name": name if name is not None else inner.__name__,
            "table_name": table_name,
            "primary_key": _key_hint(primary_key),
            "merge_key": _key_hint(merge_key),
            "write_disposition": write_disposition,
            "schema_contract": schema_contract,
            "selected": selected,
            "parallelized": parallelized,
        }
        if incremental is not None:
            metadata["incremental"] = incremental.to_metadata()
        setattr(inner, "__cdf_dlt_metadata__", metadata)
        return inner

    if func is not None:
        return decorate(func)
    return decorate


@overload
def source(func: S, /) -> S: ...


@overload
def source(
    *,
    name: str | None = None,
    schema_contract: str | Mapping[str, str] | None = None,
    parallelized: bool = False,
) -> Callable[[S], S]: ...


def source(
    func: S | None = None,
    /,
    *,
    name: str | None = None,
    schema_contract: str | Mapping[str, str] | None = None,
    parallelized: bool = False,
) -> S | Callable[[S], S]:
    def decorate(inner: S) -> S:
        source_name = name if name is not None else inner.__name__
        metadata: dict[str, object] = {
            "kind": "source",
            "name": source_name,
            "schema_contract": schema_contract,
            "parallelized": parallelized,
        }
        setattr(inner, "__cdf_dlt_metadata__", metadata)
        setattr(inner, "__cdf_dlt_source_name__", source_name)
        return inner

    if func is not None:
        return decorate(func)
    return decorate


def bind_source(resource_func: R, source_name: str) -> R:
    metadata = getattr(resource_func, "__cdf_dlt_metadata__", None)
    if isinstance(metadata, dict):
        metadata["source_name"] = source_name
    return resource_func


def _key_hint(value: str | Sequence[str] | None) -> str | tuple[str, ...] | None:
    if value is None:
        return None
    if isinstance(value, str):
        return value
    return tuple(value)
