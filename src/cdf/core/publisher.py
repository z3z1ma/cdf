import typing as t
from dataclasses import dataclass

from dlt.common.configuration import with_config
from sqlglot import exp

import cdf.core.constants as c

if t.TYPE_CHECKING:
    import pandas as pd


class Payload(t.NamedTuple):
    """A payload to publish sent to the first pos arg of a publisher."""

    payload: "pd.DataFrame"
    last_execution_time: str | None = None


class _Runner(t.Protocol):
    __wrapped__: t.Callable[..., None]

    def __call__(self, data: Payload, **kwargs) -> None:
        ...


@dataclass
class publisher_spec:
    name: str
    runner: _Runner
    from_model: str
    mapping: t.Dict[str, str]
    version: int = 1
    owners: t.Sequence[str] = ()
    description: str = ""
    tags: t.Sequence[str] = ()
    cron: str | None = None
    enabled: bool = True

    def __post_init__(self) -> None:
        projection = (
            [exp.column(name).as_(alias) for name, alias in self.mapping.items()]
            if self.mapping
            else [exp.Star()]
        )
        self.query = exp.select(*projection).from_(self.from_model)
        runner = self.runner
        self.runner = with_config(
            runner, sections=("publishers", runner.__module__, runner.__name__)
        )
        self.runner.__wrapped__ = runner

    def __call__(self, data: Payload, **kwargs) -> None:
        self.runner(data, **kwargs)


def export_publishers(*publishers: publisher_spec, scope: dict | None = None) -> None:
    """Export publishers to the callers global scope.

    Args:
        *publishers (publisher_spec): The publishers to export.
        scope (dict | None, optional): The scope to export to. Defaults to globals().
    """
    if scope is None:
        import inspect

        frame = inspect.currentframe()
        if frame is not None:
            frame = frame.f_back
        if frame is not None:
            scope = frame.f_globals

    (scope or globals()).setdefault(c.CDF_PUBLISHERS, []).extend(publishers)


__all__ = ["Payload", "publisher_spec", "export_publishers"]
