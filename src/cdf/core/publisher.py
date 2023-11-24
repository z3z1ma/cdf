import typing as t
from dataclasses import dataclass

from dlt.common.configuration import with_config
from sqlglot import exp

import cdf.core.constants as c

if t.TYPE_CHECKING:
    import pandas as pd


class _Runner(t.Protocol):
    __wrapped__: t.Callable[..., None]

    def __call__(self, data: "pd.DataFrame", **kwargs) -> None:
        ...


@dataclass
class publisher_spec:
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

    def __call__(self, data: "pd.DataFrame", **kwargs) -> None:
        self.runner(data, **kwargs)


def export_publishers(
    *, scope: dict | None = None, **publishers: publisher_spec
) -> None:
    """Export publishers to the global scope.

    Args:
        scope (dict | None, optional): The scope to export to. Defaults to globals().
        **publishers (publisher_spec): The publishers to export.
    """
    if scope is None:
        import inspect

        frame = inspect.currentframe()
        if frame is not None:
            frame = frame.f_back
        if frame is not None:
            scope = frame.f_globals

    (scope or globals()).setdefault(c.CDF_PUBLISHER, {}).update(publishers)
