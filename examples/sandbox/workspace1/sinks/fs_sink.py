import dlt
from sqlmesh.core.config import GatewayConfig, parse_connection_config

ingest = dlt.destinations.filesystem(
    "file://_storage", layout="{table_name}/{load_id}.{file_id}.{ext}.gz"
)

transform = GatewayConfig(
    connection=parse_connection_config(
        {"type": "duckdb", "database": "cdf.duckdb", "extensions": ["httpfs"]}
    ),
    state_schema="_cdf_state",
)
