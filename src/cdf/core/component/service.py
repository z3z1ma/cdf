import typing as t

from .base import Component

ServiceProto = t.Any


class Service(Component[ServiceProto], frozen=True):
    """A service that the workspace provides. IE an API, database, requests client, etc."""
