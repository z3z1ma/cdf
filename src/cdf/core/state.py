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


JSON = t.Union[bool, int, float, str, t.List["JSON"], t.Dict[str, "JSON"]]


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
    def kv_table(self) -> exp.Table:
        """The name of the key-value table"""
        return exp.table_("json_store", self.schema_)

    @property
    def adapter(self) -> EngineAdapter:
        """The adapter to the state store"""
        if self._adapter is None:
            adapter = self.connection.create_engine_adapter()
            adapter.create_schema(self.kv_table.sql())
            D = exp.DataType.build
            adapter.create_table(
                self.kv_table,
                {"key": D("text"), "value": D("text")},
            )
            self._adapter = adapter
        return self._adapter

    def _execute(self, sql: str) -> None:
        """Execute a SQL statement"""
        self.adapter.execute(sql)

    def store_json(self, key: str, value: JSON) -> None:
        """Store a JSON value"""
        with self.adapter.transaction(value is not None):
            self.adapter.delete_from(self.kv_table, f"key = '{key}'")
            if value is not None:
                self.adapter.insert_append(
                    self.kv_table,
                    pd.DataFrame([{"key": key, "value": json.dumps(value)}]),
                )

    def load_json(self, key: str) -> JSON:
        """Load a JSON value"""
        return json.loads(
            self.adapter.fetchone(
                exp.select("value").from_(self.kv_table).where(f"key = '{key}'")
            )[0]
        )

    __getitem__ = load_json
    __setitem__ = store_json

    def __enter__(self, condition: bool = True) -> "StateStore":
        """Proxies to the transaction context manager"""
        self.__trans = self.adapter.transaction(condition)
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        """Proxies to the transaction context manager"""
        self.__trans.__exit__(exc_type, exc_value, traceback)

    def __del__(self) -> None:
        """Close the connection to the state store"""
        if self._adapter is not None:
            self.adapter.close()
