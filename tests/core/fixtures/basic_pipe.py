"""
PIPELINE (
    name data_pipeline, -- Name of the pipeline
    description 'Load data from source',
    tags [pii, main],
    cron '0 0 * * *',
    owner 'jdoe'
);

--@METRIC("account_*", row_count); -- Number of rows
--@FILTER("account_*", pii_filter); -- Sensitive data
"""
import dlt


def foo(n: int) -> int:
    return n + 1


def func(x: int):
    yield from range(foo(x))


pipeline = dlt.pipeline("test")
pipeline.run(func, destination="duckdb", table_name="data_pipeline")

with pipeline.sql_client() as client:
    client.execute("SELECT * FROM test.data_pipeline").fetchall()
