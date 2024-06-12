import dlt
import cdf
import duckdb


p = (
    cdf.find_nearest(__file__)
    .bind(lambda p: p.get_workspace("alex"))
    .map(lambda w: w.path / "cdf.duckdb")
    .unwrap()
)

LOCALDB = str(p)

conn = duckdb.connect(LOCALDB)
conn.install_extension("httpfs")
conn.load_extension("httpfs")
conn.close()


ingest = dlt.destinations.duckdb(LOCALDB)

stage = dlt.destinations.filesystem(
    "file://_storage",
    layout="{table_name}/{load_id}.{file_id}.{ext}.gz",
)

transform = dict(
    connection=cdf.transform_connection(
        "duckdb", database=LOCALDB, extensions=["httpfs"]
    )
)
