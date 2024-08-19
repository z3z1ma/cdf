import typing as t

from .base import Entrypoint

DataPublisherProto = t.Tuple[
    t.Callable[..., None],  # run
    t.Callable[..., bool],  # preflight
    t.Optional[t.Callable[..., None]],  # success hook
    t.Optional[t.Callable[..., None]],  # failure hook
]


class DataPublisher(
    Entrypoint[DataPublisherProto],
    frozen=True,
):
    """A data publisher which pushes data to an operational system."""

    def __call__(self, *args: t.Any, **kwargs: t.Any) -> None:
        """Publish the data"""
        publisher, pre, success, err = self.main(*args, **kwargs)
        if not pre():
            raise ValueError("Preflight check failed")
        try:
            return publisher()
        except Exception as e:
            if err:
                err()
            raise e
        else:
            if success:
                success()
