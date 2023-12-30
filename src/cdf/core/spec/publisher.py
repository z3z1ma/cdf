"""The spec classes for continuous data framework publishers."""
import typing as t

import pydantic
from sqlglot import exp

import cdf.core.constants as c
from cdf.core.spec.base import ComponentSpecification, Packageable, Schedulable

if t.TYPE_CHECKING:
    import pandas as pd
    import sqlmesh


class PublisherInterface(t.Protocol):
    def __call__(
        self,
        df: "pd.DataFrame",
        last_execution_time: str | None = None,
        **kwargs: t.Any,
    ) -> int:
        ...


class PublisherSpecification(ComponentSpecification, Packageable, Schedulable):
    """A publisher specification."""

    select: t.Dict[str, str]
    """Column name mapping used to translate column names to the API names declaratively."""
    from_: t.Annotated[str, pydantic.Field(alias="from")]
    """The model to publish from."""
    where: str | None = None
    """A predicate to filter the data to publish."""
    affinity: t.List[str] = []
    """The sinks from which this publisher can source data."""

    _key = c.PUBLISHERS

    @property
    def pub(self) -> PublisherInterface:
        """The publisher function."""
        return self._main

    @pydantic.field_validator("affinity", mode="after")
    @classmethod
    def _affinity_validator(cls, affinity: t.List[str]) -> t.List[str]:
        if not affinity:
            raise ValueError("Publisher must have at least one affinity.")
        return affinity

    @property
    def query(self) -> exp.Select:
        """The query to run."""
        projection = (
            [
                exp.column(name).as_(alias, quoted=True)
                for name, alias in self.select.items()
            ]
            if self.select
            else [exp.Star()]
        )
        return exp.select(*projection).from_(self.from_).where(self.where)

    def __call__(self, context: "sqlmesh.Context", safe: bool = False, **kwargs) -> int:
        """Run the publisher.

        Args:
            context (Context): The sqlmesh context to use.
            safe (bool): Whether to check that the model is managed by the cdf transformation layer
                before publishing. Defaults to False. This will throw an error if the model is not managed.
            **kwargs: The kwargs to forward to the publisher.

        Returns:
            int: The number of rows published to the target system.
        """
        if context.config.default_gateway not in self.affinity:
            raise ValueError(
                f"Publisher {self.name} cannot publish from {context.config.default_gateway}."
            )
        if self.from_ not in context.models and safe:
            raise ValueError(
                f"Publisher {self.name} will not publish from {self.from_} because it is not managed by the"
                " cdf transformation layer and thus its integrity cannot be guaranteed."
            )

        df = context.fetchdf(self.query, quote_identifiers=True)
        if df.empty:
            return 0

        # TODO: Add last_execution_time to the payload. We need to track it via some data pipeline state.
        return self.pub(df, **kwargs)


__all__ = ["PublisherSpecification", "PublisherInterface"]
