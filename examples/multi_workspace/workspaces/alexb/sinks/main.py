import cdf

LOCALDB = "cdf.duckdb"


@cdf.with_config(sections=("bigquery", "prod"))
def _prod(credentials: str = cdf.inject_secret):
    return cdf.destination.bigquery(credentials)  # type: ignore


@cdf.with_config(sections=("bigquery", "staging"))
def _prod_staging(credentials: str = cdf.inject_secret):
    return cdf.destination.filesystem(
        "gs://harness_analytics_staging/cdf",
        credentials,
        layout="{table_name}/{load_id}.{file_id}.{ext}",
    )


def local() -> tuple:
    """An example entrypoint for a CDF sink"""
    import duckdb

    conn = duckdb.connect(LOCALDB)
    conn.install_extension("httpfs")
    conn.load_extension("httpfs")

    return (
        cdf.destination.duckdb(conn),
        None,
        cdf.gateway.parse_obj(
            {
                "connection": {
                    "type": "duckdb",
                    "database": LOCALDB,
                    "extensions": ["httpfs"],
                }
            }
        ),
    )


def prod() -> tuple:
    """An example entrypoint for a CDF sink with config injection and separated funcs"""
    return _prod(), _prod_staging(), None
