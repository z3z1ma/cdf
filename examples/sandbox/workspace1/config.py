import getpass
import os

import sqlmesh

import cdf

workspace = "workspace1"
project = cdf.get_project(
    os.path.dirname(__file__),
).unwrap()
config = sqlmesh.Config.model_validate(
    dict(
        gateways=cdf.get_gateways(project, workspace),
        project=workspace,
        default_gateway="local",
        model_defaults={
            "dialect": "duckdb",
            "start": "2020-01-01",
        },
        plan={
            "auto_categorize_changes": {
                "sql": "full",
                "seed": "semi",
                "external": "semi",
            }
        },
        username=getpass.getuser(),
        physical_schema_override={},
        notification_targets=[],
        format={
            "normalize": True,
            "pad": 4,
            "indent": 4,
            "normalize_functions": "lower",
            "leading_comma": False,
            "max_text_width": 120,
            "append_newline": True,
        },
        ui={"format_on_save": True},
    )
)
