"""The state module is responible for providing an adapter through which we can persist data"""

import typing as t

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
    """The connection configuration to the state store"""

    _adapter: t.Optional[EngineAdapter] = None
    """Lazy loaded adapter to the state store"""

    @property
    def adapter(self) -> EngineAdapter:
        """The adapter to the state store"""
        if self._adapter is None:
            self._adapter = self.connection.create_engine_adapter()
        return self._adapter

    def __del__(self) -> None:
        """Close the connection to the state store"""
        if self._adapter is not None:
            self.adapter.close()
