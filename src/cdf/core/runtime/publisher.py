"""The runtime publisher module is responsible for executing publishers from publisher specifications.

It performs the following functions:
- Validates the dependencies of the publisher exist.
- Verifies the dependencies are up-to-date.
- Executes the publisher script.
"""

import datetime
import typing as t

import sqlmesh
from sqlmesh.core.dialect import normalize_model_name

import cdf.core.logger as logger
from cdf.core.runtime.common import with_activate_project
from cdf.core.specification import PublisherSpecification
from cdf.core.state import with_audit
from cdf.types import M


@with_activate_project
@with_audit(
    "execute_publisher",
    lambda spec, transform_ctx, skip_verification=False: {
        "name": spec.name,
        "owner": spec.owner,
        "depends_on": spec.depends_on,
        "skipped_verification": skip_verification,
        "gateway": transform_ctx.gateway,
        "workspace": spec.workspace.name,
        "project": spec.project.name,
    },
)
def execute_publisher_specification(
    spec: PublisherSpecification,
    transform_ctx: sqlmesh.Context,
    skip_verification: bool = False,
) -> M.Result[t.Dict[str, t.Any], Exception]:
    """Execute a publisher specification.

    Args:
        spec: The publisher specification to execute.
        transform_ctx: The SQLMesh context to use for execution.
        skip_verification: Whether to skip the verification of the publisher dependencies.
    """
    if not skip_verification:
        models = transform_ctx.models
        for dependency in spec.depends_on:
            normalized_name = normalize_model_name(
                dependency, transform_ctx.default_catalog, transform_ctx.default_dialect
            )
            if normalized_name not in models:
                return M.error(
                    ValueError(
                        f"Cannot find tracked dependency {dependency} in models."
                    )
                )
            model = models[normalized_name]
            snapshot = transform_ctx.get_snapshot(normalized_name)
            if not snapshot:
                return M.error(ValueError(f"Snapshot not found for {normalized_name}"))
            if snapshot.missing_intervals(
                datetime.date.today() - datetime.timedelta(days=7),
                datetime.date.today() - datetime.timedelta(days=1),
            ):
                return M.error(
                    ValueError(f"Model {model} has missing intervals. Cannot publish.")
                )
            logger.info(f"Model {model} has no missing intervals.")
        logger.info("All tracked dependencies passed interval check.")
    else:
        logger.warning("Skipping dependency verification.")
    try:
        return M.ok(spec())
    except Exception as e:
        logger.error(f"Error running publisher script {spec.path}: {e}")
        return M.error(e)


__all__ = ["execute_publisher_specification"]
