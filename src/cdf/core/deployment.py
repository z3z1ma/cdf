"""Module for managing deployment strategies for a project."""

from __future__ import annotations

import typing as t

from cdf.core.project import Project


@t.final
class DeploymentManager:
    """Manages deployment strategies for a project."""

    def __init__(self, project: Project):
        self.project = project

    def deploy(self, strategy: str, **kwargs: t.Any) -> None:
        """Deploy the project or data package using the specified strategy."""
        if strategy == "local_cron":
            self._deploy_local_cron(**kwargs)
        elif strategy == "k8s_cron":
            self._deploy_k8s_cron(**kwargs)
        elif strategy == "docker":
            self._deploy_docker(**kwargs)
        elif strategy == "harness_ci":
            self._deploy_harness_ci(**kwargs)
        else:
            raise ValueError(f"Unsupported deployment strategy: {strategy}")

    def _deploy_local_cron(self, **kwargs: t.Any):
        """Implementation for local cron deployment"""
        _ = kwargs

    def _deploy_k8s_cron(self, **kwargs: t.Any):
        """Implementation for Kubernetes cron job deployment"""
        _ = kwargs

    def _deploy_docker(self, **kwargs: t.Any):
        """Implementation for Docker image deployment"""
        _ = kwargs

    def _deploy_harness_ci(self, **kwargs: t.Any):
        """Implementation for Harness CI pipeline deployment"""
        _ = kwargs
