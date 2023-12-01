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
    """The name of the publisher."""
    runner: _Runner
    """The publisher function. The first pos arg will be the data to publish."""
    from_model: str
    """The model to publish from."""
    mapping: t.Dict[str, str]
    """Column name remapping used to translate column names to the API names declaratively."""
    version: int = 1
    """The version of the publisher. Used to track execution history."""
    owners: t.Sequence[str] = ()
    """The owners of the publisher."""
    description: str = ""
    """The description of the publisher."""
    tags: t.Sequence[str] = ()
    """Tags for this publisher used for component queries."""
    cron: str | None = None
    """A cron expression for scheduling this publisher."""
    enabled: bool = True
    """Whether this publisher is enabled."""

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
        """Run the publisher.

        Args:
            data (Payload): The data to publish to an external system.
            **kwargs: The kwargs to forward to the publisher.
        """
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
