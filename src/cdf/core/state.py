"""The state module is responible for providing an adapter through which we can persist data"""

import typing as t

from sqlmesh.core.config.connection import (
    DuckDBConnectionConfig,
    MySQLConnectionConfig,
    PostgresConnectionConfig,
)

DEFAULT_STATE_CONN = DuckDBConnectionConfig(database=".cdf.duckdb")


StateConfig = t.Union[
    DuckDBConnectionConfig,
    MySQLConnectionConfig,
    PostgresConnectionConfig,
]
