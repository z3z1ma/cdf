import getpass

import sqlmesh

import cdf

project = cdf.find_nearest(__file__).unwrap()
workspace = "workspace1"

config = sqlmesh.Config.model_validate(
    dict(
        gateways=cdf.get_gateways(project, workspace).unwrap(),
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
