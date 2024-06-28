import time
import typing as t

from sqlmesh.core.notification_target import (
    ConsoleNotificationTarget,
    NotificationEvent,
    NotificationStatus,
)
from sqlmesh.utils.errors import AuditError

import cdf.core.logger as logger
from cdf.core.project import Workspace


class CDFNotificationTarget(ConsoleNotificationTarget):
    """A notification target which sends notifications to the state of a CDF workspace."""

    workspace: Workspace
    notify_on: t.FrozenSet[NotificationEvent] = frozenset(
        {
            NotificationEvent.APPLY_START,
            NotificationEvent.APPLY_END,
            NotificationEvent.RUN_START,
            NotificationEvent.RUN_END,
            NotificationEvent.MIGRATION_START,
            NotificationEvent.MIGRATION_END,
            NotificationEvent.APPLY_FAILURE,
            NotificationEvent.RUN_FAILURE,
            NotificationEvent.AUDIT_FAILURE,
            NotificationEvent.MIGRATION_FAILURE,
        }
    )

    _run_start: float = 0.0
    """The time a run started"""
    _apply_start: float = 0.0
    """The time an apply started"""
    _migrate_start: float = 0.0
    """The time a migration started"""

    def send(
        self, notification_status: NotificationStatus, msg: str, **kwargs: t.Any
    ) -> None:
        msg += "\n(event logged in state store)"
        if notification_status.is_failure:
            logger.error(msg)
        elif notification_status.is_warning:
            logger.warning(msg)
        else:
            logger.info(msg)

    def notify_run_start(self, environment: str) -> None:
        """Notify the workspace of a run start"""
        self._run_start = time.time()
        self.workspace.state.audit(
            "sqlmesh_run_start",
            success=True,
            environment=environment,
        )

    def notify_run_end(self, environment: str) -> None:
        """Notify the workspace of a run end"""
        self.workspace.state.audit(
            "sqlmesh_run_end",
            success=True,
            environment=environment,
            elapsed=time.time() - self._run_start,
        )

    def notify_run_failure(self, exc: str) -> None:
        """Notify the workspace of a run failure"""
        self.workspace.state.audit(
            "sqlmesh_run_failure",
            success=False,
            error=exc,
            elapsed=time.time() - self._run_start,
        )

    def notify_apply_start(self, environment: str, plan_id: str) -> None:
        """Notify the workspace of an apply start"""
        self._apply_start = time.time()
        self.workspace.state.audit(
            "sqlmesh_apply_start",
            success=True,
            environment=environment,
            plan_id=plan_id,
        )

    def notify_apply_end(self, environment: str, plan_id: str) -> None:
        """Notify the workspace of an apply end"""
        self.workspace.state.audit(
            "sqlmesh_apply_end",
            success=True,
            environment=environment,
            plan_id=plan_id,
            elapsed=time.time() - self._apply_start,
        )

    def notify_apply_failure(self, environment: str, plan_id: str, exc: str) -> None:
        """Notify the workspace of an apply failure"""
        self.workspace.state.audit(
            "sqlmesh_apply_failure",
            success=False,
            environment=environment,
            plan_id=plan_id,
            error=exc,
            elapsed=time.time() - self._apply_start,
        )

    def notify_migration_start(self) -> None:
        """Notify the workspace of a migration start"""
        self._migrate_start = time.time()
        self.workspace.state.audit(
            "sqlmesh_migration_start",
            success=True,
        )

    def notify_migration_end(self) -> None:
        """Notify the workspace of a migration end"""
        self.workspace.state.audit(
            "sqlmesh_migration_end",
            success=True,
            elapsed=time.time() - self._migrate_start,
        )

    def notify_migration_failure(self, exc: str) -> None:
        """Notify the workspace of a migration failure"""
        self.workspace.state.audit(
            "sqlmesh_migration_failure",
            success=False,
            error=exc,
            elapsed=time.time() - self._migrate_start,
        )

    def notify_audit_failure(self, audit_error: AuditError) -> None:
        """Notify the workspace of an audit failure"""
        self.workspace.state.audit(
            "sqlmesh_audit_failure",
            success=False,
            sql=audit_error.sql(),
            name=audit_error.audit_name,
            model=audit_error.model_name,  # type: ignore
            err_msg=str(audit_error),
            elapsed=1.0,
        )
