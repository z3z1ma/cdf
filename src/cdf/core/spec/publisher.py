"""The spec classes for continuous data framework publishers."""
import time
import typing as t

import pydantic
from sqlglot import exp

import cdf.core.constants as c
import cdf.core.logger as logger
from cdf.core.spec.base import (
    ComponentSpecification,
    Executable,
    Packageable,
    Schedulable,
)

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


class PublisherSpecification(
    ComponentSpecification, Executable, Packageable, Schedulable
):
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
        return self.main

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

    def __call__(
        self, context: "sqlmesh.Context", strict: bool = False, **kwargs
    ) -> int:
        """Run the publisher.

        Args:
            context (Context): The sqlmesh context to use.
            strict (bool): If set to true, we assert that the publisher spec's `from` is a model known to
                the cdf transformation layer (aka a managed model). This ensures more controls, audits, and
                user-input is present ensuring safer publish operations. Defaults to false.
            **kwargs: The kwargs to forward to the publisher.

        Returns:
            int: The number of rows published to the target system.
        """
        if context.config.default_gateway not in self.affinity:
            raise ValueError(
                f"Publisher {self.name} cannot publish from sink `{context.config.default_gateway}`."
            )
        if self.from_ not in context.models and strict:
            raise ValueError(
                f"Publisher {self.name} will not publish from `{self.from_}` because it is not managed by the"
                " cdf transformation layer and thus its integrity cannot be guaranteed."
            )

        logger.debug(self.query.sql(dialect=context.config.dialect))
        logger.info("Executing query")
        querystart = time.perf_counter()
        df = context.fetchdf(self.query, quote_identifiers=True)
        queryend = time.perf_counter()
        logger.info("Fetched %d rows in %.3f seconds", len(df), queryend - querystart)
        if df.empty:
            return 0

        # TODO: Add last_execution_time to the payload. We need to track it via some data pipeline state.
        logger.info("Publishing data")
        pubstart = time.perf_counter()
        records_affected = self.pub(df, **kwargs)
        pubend = time.perf_counter()
        logger.info(
            "Published %d rows in %.3f seconds (throughput %.2f/s)",
            records_affected,
            pubend - pubstart,
            max(records_affected / (pubend - pubstart), 0),
        )
        return records_affected


__all__ = ["PublisherSpecification", "PublisherInterface"]
