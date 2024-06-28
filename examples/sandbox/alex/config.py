import cdf
import sqlmesh
from cdf.integrations.sqlmesh import CDFNotificationTarget

workspace = cdf.get_workspace(__file__).unwrap()

config = sqlmesh.Config.model_validate(
    dict(
        gateways=dict(workspace.get_transform_gateways()),
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
        # username=getpass.getuser(),
        physical_schema_override={},
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

config.notification_targets = [CDFNotificationTarget(workspace=workspace)]
