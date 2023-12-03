import cdf

LOCAL_DB = "cdf.duckdb"


@cdf.with_config(sections=("bigquery", "prod"))
def prod(credentials: str = cdf.secret):
    return cdf.destination.bigquery(
        credentials,  # type: ignore
        # http_timeout
        # file_upload_timeout
        # retry_deadline
        # replace_strategy
    )


def development():
    import duckdb

    conn = duckdb.connect(LOCAL_DB)
    conn.install_extension("httpfs")
    conn.load_extension("httpfs")
    return cdf.destination.duckdb(conn)


@cdf.with_config(sections=("bigquery", "staging"))
def prod_staging(credentials: str = cdf.secret):
    return cdf.destination.filesystem(
        "gs://harness_analytics_staging/cdf",
        credentials,
        layout="{table_name}/{load_id}.{file_id}.{ext}",
    )


__CDF_SINKS__ = [
    cdf.sink_spec(
        name="bq",
        environment="prod",
        destination=prod,
        staging=prod_staging,
    ),
    cdf.sink_spec(
        name="local",
        environment="dev",
        destination=development,
        gateway=cdf.gateway.parse_obj(
            {
                "connection": {
                    "type": "duckdb",
                    "database": LOCAL_DB,
                    "extensions": ["httpfs"],
                }
            }
        ),
    ),
]
