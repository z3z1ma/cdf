import sqlmesh

import cdf

workspace = "alexb"
project = cdf.find_nearest().unwrap()
config = sqlmesh.Config.model_validate(
    dict(
        gateways={
            "local": cdf.get_gateway(project, workspace, "local").unwrap(),
        },
        model_defaults={"dialect": "duckdb"},
    )
)
