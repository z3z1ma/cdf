import typing as t

from .base import Component


class Service(Component[t.Any], frozen=True):
    """A service that the workspace provides. IE an API, database, requests client, etc."""
