import typing as t
from dataclasses import dataclass

from dlt.common.configuration import with_config
from sqlglot import exp

if t.TYPE_CHECKING:
    import pandas as pd


class PublisherData(t.NamedTuple):
    """A payload to publish sent to the first pos arg of a publisher."""

    df: "pd.DataFrame"
    last_execution_time: str | None = None


class Publisher(t.Protocol):
    def __call__(self, data: PublisherData, **kwargs: t.Any) -> None:
        ...


@dataclass
class publisher_spec:
    """A publisher specification."""

    name: str
    """The name of the publisher."""
    runner: Publisher
    """The publisher function. The first pos arg will be the data to publish."""
    select: t.Dict[str, str]
    """Column name mapping used to translate column names to the API names declaratively."""
    from_: str
    """The model to publish from."""
    where: str | None = None
    """A predicate to filter the data to publish."""
    affinity: t.Sequence[str] = ()
    """The sinks from which this publisher can source data."""
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
            [
                exp.column(name).as_(alias, quoted=True)
                for name, alias in self.select.items()
            ]
            if self.select
            else [exp.Star()]
        )
        self.query = exp.select(*projection).from_(self.from_).where(self.where)
        runner = self.runner
        self.runner = with_config(
            runner, sections=("publishers", runner.__module__, runner.__name__)
        )
        self.runner.__wrapped__ = runner  # type: ignore

    def __call__(self, data: PublisherData, **kwargs) -> None:
        """Run the publisher.

        Args:
            data (Payload): The data to publish to an external system.
            **kwargs: The kwargs to forward to the publisher.
        """
        self.runner(data, **kwargs)


__all__ = ["PublisherData", "publisher_spec"]
