import atexit

import dlt
import duckdb
from sqlmesh.core.config import GatewayConfig, parse_connection_config

LOCALDB = "cdf.duckdb"

conn = duckdb.connect(LOCALDB)
conn.install_extension("httpfs")
conn.load_extension("httpfs")


ingest = dlt.destinations.duckdb(conn)

stage = dlt.destinations.filesystem(
    "file://_storage", layout="{table_name}/{load_id}.{file_id}.{ext}.gz"
)

transform = GatewayConfig(
    connection=parse_connection_config(
        {"type": "duckdb", "database": LOCALDB, "extensions": ["httpfs"]}
    )
)

atexit.register(conn.close)
