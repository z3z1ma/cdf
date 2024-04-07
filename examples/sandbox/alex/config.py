import getpass

import sqlmesh

import cdf

workspace = cdf.get_workspace_from_path(__file__).unwrap()

config = sqlmesh.Config.model_validate(
    dict(
        gateways=workspace.get_gateways().unwrap(),
        project=workspace.name,
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
