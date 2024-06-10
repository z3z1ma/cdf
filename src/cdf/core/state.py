"""The state module is responible for providing an adapter through which we can persist data"""

import typing as t
from functools import cached_property

import pydantic
from sqlmesh.core.config.connection import (
    DuckDBConnectionConfig,
    MySQLConnectionConfig,
    PostgresConnectionConfig,
)
from sqlmesh.core.engine_adapter import EngineAdapter


class StateStore(pydantic.BaseModel):
    """The state store is responsible for persisting data"""

    connection: t.Union[
        DuckDBConnectionConfig,
        MySQLConnectionConfig,
        PostgresConnectionConfig,
    ] = DuckDBConnectionConfig(database=".cdf_state.db")

    @cached_property
    def adapter(self) -> EngineAdapter:
        """Check if the state store is available"""
        return self.connection.create_engine_adapter()
