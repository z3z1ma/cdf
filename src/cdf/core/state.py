"""The state module is responible for providing an adapter through which we can persist data"""

import json
import typing as t

import pandas as pd
import pydantic
from sqlglot import exp
from sqlmesh.core.config.connection import (
    DuckDBConnectionConfig,
    MySQLConnectionConfig,
    PostgresConnectionConfig,
)
from sqlmesh.core.engine_adapter import EngineAdapter


class StateStore(pydantic.BaseModel):
    """The state store is responsible for persisting data"""

    model_config = {"frozen": True, "from_attributes": True}

    schema_: t.Annotated[str, pydantic.Field(alias="schema")] = "cdf"
    """The schema in which to store data"""
    protected: bool = True
    """Whether the state store is protected, i.e. should never be torn down

    A safety measure to prevent accidental data loss when users are consuming the cdf API
    directly. This should be set to False when running tests or you know what you're doing.
    """

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

    def setup(self) -> None:
        """Setup the state store"""
        self.adapter.create_schema(self.schema_)

    def teardown(self) -> None:
        """Teardown the state store"""
        if not self.protected:
            self.adapter.drop_schema(self.schema_)

    def _execute(self, sql: str) -> None:
        """Execute a SQL statement"""
        self.adapter.execute(sql)

    def store_json(self, key: str, value: t.Any) -> None:
        """Store a JSON value"""
        D = exp.DataType.build
        self.adapter.create_state_table(
            "json_store", {"key": D("text"), "value": D("text")}
        )
        self.adapter.insert_append(
            "json_store", pd.DataFrame([{"key": key, "value": json.dumps(value)}])
        )

    def __del__(self) -> None:
        """Close the connection to the state store"""
        if self._adapter is not None:
            self.adapter.close()
