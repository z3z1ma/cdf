"""
SINK (
    name local,
    description 'Local sink to DuckDB'
);
"""
import dlt
import duckdb
from sqlmesh.core.config import GatewayConfig, parse_connection_config

LOCALDB = "cdf.duckdb"

conn = duckdb.connect(LOCALDB)
conn.install_extension("httpfs")
conn.load_extension("httpfs")


sink = (
    dlt.destinations.duckdb(conn),
    None,
    GatewayConfig(
        connection=parse_connection_config(
            {"type": "duckdb", "database": LOCALDB, "extensions": ["httpfs"]}
        )
    ),
)
