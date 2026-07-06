"""Protocols implemented by the firn Python host context."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Protocol


class HttpResponse(Protocol):
    @property
    def status_code(self) -> int: ...

    @property
    def headers(self) -> Mapping[str, str]: ...

    def json(self) -> object: ...

    @property
    def text(self) -> str: ...


class HttpClient(Protocol):
    def request(
        self,
        method: str,
        url: str,
        *,
        headers: Mapping[str, str] | None = None,
        params: Mapping[str, object] | None = None,
        json: object | None = None,
    ) -> HttpResponse: ...

    def get(
        self,
        url: str,
        *,
        headers: Mapping[str, str] | None = None,
        params: Mapping[str, object] | None = None,
    ) -> HttpResponse: ...


class SecretProvider(Protocol):
    def get(self, uri: str, /) -> str: ...


class CursorView(Protocol):
    def get(self, field: str, default: object | None = None, /) -> object | None: ...


class Logger(Protocol):
    def debug(self, message: str, *, extra: Mapping[str, object] | None = None) -> None: ...

    def info(self, message: str, *, extra: Mapping[str, object] | None = None) -> None: ...

    def warning(self, message: str, *, extra: Mapping[str, object] | None = None) -> None: ...

    def error(self, message: str, *, extra: Mapping[str, object] | None = None) -> None: ...


class Context(Protocol):
    @property
    def http(self) -> HttpClient: ...

    @property
    def secrets(self) -> SecretProvider: ...

    @property
    def cursor(self) -> CursorView: ...

    @property
    def logger(self) -> Logger: ...
