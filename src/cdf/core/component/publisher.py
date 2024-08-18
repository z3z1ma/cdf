import typing as t

import pydantic

from .base import Entrypoint, _get_bind_func, _unwrap_entrypoint


def _ping() -> bool:
    """A default preflight check which always returns True."""
    return bool("pong")


class DataPublisher(Entrypoint[t.Any], frozen=True):
    """A data publisher which pushes data to an operational system."""

    preflight_check: t.Callable[..., bool] = _ping
    """A user defined function to check if the data publisher is able to publish data"""

    integration_test: t.Optional[t.Callable[..., bool]] = None
    """A function to test the data publisher in an integration environment"""

    @pydantic.field_validator("preflight_check", "integration_test", mode="before")
    @classmethod
    def _bind_ancillary(cls, value: t.Any, info: pydantic.ValidationInfo) -> t.Any:
        """Bind the active workspace to the ancillary functions."""
        return _get_bind_func(info)(_unwrap_entrypoint(value))

    def __call__(self, *args: t.Any, **kwargs: t.Any) -> None:
        """Publish the data"""
        if not self.preflight_check():
            raise RuntimeError("Preflight check failed")
        return self.main(*args, **kwargs)
