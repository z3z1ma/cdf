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


ingest = dlt.destinations.duckdb(conn)

staging = None

transform = GatewayConfig(
    connection=parse_connection_config(
        {"type": "duckdb", "database": LOCALDB, "extensions": ["httpfs"]}
    )
)
