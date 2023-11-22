import typing as t
from dataclasses import dataclass


@dataclass
class CDFPublisherWrapper:
    runner: t.Callable[..., None]
    from_model: str
    mapping: t.Dict[str, str]
    version: int = 1
    owners: t.Sequence[str] = ()
    description: str = ""
    tags: t.Sequence[str] = ()
    cron: str | None = None
    enabled = True
