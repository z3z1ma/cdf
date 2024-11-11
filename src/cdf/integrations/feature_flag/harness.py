"""Harness feature flag provider."""

from __future__ import annotations

import logging
import os
import typing as t
from concurrent.futures import ThreadPoolExecutor

import dlt
from dlt.common.configuration import with_config
from dlt.sources import DltSource
from dlt.sources.helpers import requests
from featureflags.client import CfClient, Config, Target
from featureflags.evaluations.feature import FeatureConfigKind
from featureflags.interface import Cache
from featureflags.util import log as _ff_logger

from cdf.integrations.feature_flag.base import (
    AbstractFeatureFlagAdapter,
    FlagAdapterResponse,
)

logger = logging.getLogger(__name__)


# This exists because the default harness LRU implementation does not store >1000 flags
# The interface is mostly satisfied by dict, so we subclass it and implement the missing methods
class _HarnessCache(dict, Cache):  # type: ignore
    """A cache implementation for the harness feature flag provider."""

    def set(self, key: str, value: bool) -> None:
        self[key] = value

    def remove(self, key: str | t.List[str]) -> None:  # type: ignore
        if isinstance(key, str):
            self.pop(key, None)
        for k in key:
            self.pop(k, None)


def _quiet_logger():
    """Configure the harness FF logger to only show errors. Its too verbose otherwise."""
    _ff_logger.setLevel(logging.ERROR)


class HarnessFeatureFlagAdapter(AbstractFeatureFlagAdapter):
    _TARGET = Target("cdf")

    @with_config(sections=("feature_flags",))
    def __init__(
        self,
        sdk_key: str = dlt.secrets.value,
        api_key: str = dlt.secrets.value,
        account: str = dlt.secrets.value,
        organization: str = dlt.secrets.value,
        project: str = dlt.secrets.value,
        **kwargs: t.Any,
    ) -> None:
        """Initialize the adapter."""
        self.sdk_key = sdk_key
        self.api_key = api_key
        self.account = account
        self.organization = organization
        self.project = project
        self._pool = None
        self._client = None
        _quiet_logger()

    @property
    def client(self) -> CfClient:
        """Get the client and cache it in the instance."""
        if self._client is None:
            client = CfClient(
                sdk_key=str(self.sdk_key),
                config=Config(
                    enable_stream=False, enable_analytics=False, cache=_HarnessCache()
                ),
            )
            client.wait_for_initialization()
            self._client = client
        return self._client

    @property
    def pool(self) -> ThreadPoolExecutor:
        """Get the thread pool."""
        if self._pool is None:
            self._pool = ThreadPoolExecutor(thread_name_prefix="cdf-ff-")
        return self._pool

    def get(self, feature_name: str) -> FlagAdapterResponse:
        """Get a feature flag."""
        if feature_name not in self.get_all_feature_names():
            return FlagAdapterResponse.NOT_FOUND
        return FlagAdapterResponse.from_bool(
            self.client.bool_variation(feature_name, self._TARGET, False)
        )

    def get_all_feature_names(self) -> t.List[str]:
        """Get all the feature flags."""
        return list(
            map(lambda f: f.split("/", 1)[1], self.client._repository.cache.keys())
        )

    def _toggle(self, feature_name: str, flag: bool) -> None:
        """Toggle a feature flag."""
        if flag is self.get(feature_name).to_bool():
            return
        logger.info(f"Toggling feature flag {feature_name} to {flag}")
        requests.patch(
            f"https://app.harness.io/cf/admin/features/{feature_name}",
            headers={"x-api-key": self.api_key},
            params={
                "accountIdentifier": self.account,
                "orgIdentifier": self.organization,
                "projectIdentifier": self.project,
            },
            json={
                "instructions": [
                    {
                        "kind": "setFeatureFlagState",
                        "parameters": {"state": "on" if flag else "off"},
                    }
                ]
            },
        )

    def save(self, feature_name: str, flag: bool) -> None:
        """Create a feature flag."""
        if self.get(feature_name) is FlagAdapterResponse.NOT_FOUND:
            logger.info(f"Creating feature flag {feature_name}")
            try:
                requests.post(
                    "https://app.harness.io/cf/admin/features",
                    params={
                        "accountIdentifier": self.account,
                        "orgIdentifier": self.organization,
                    },
                    headers={
                        "Content-Type": "application/json",
                        "x-api-key": self.api_key,
                    },
                    json={
                        "defaultOnVariation": "on-variation",
                        "defaultOffVariation": "off-variation",
                        "description": "Managed by CDF",
                        "identifier": feature_name,
                        "name": feature_name.upper(),
                        "kind": FeatureConfigKind.BOOLEAN.value,
                        "permanent": True,
                        "project": self.project,
                        "variations": [
                            {"identifier": "on-variation", "value": "true"},
                            {"identifier": "off-variation", "value": "false"},
                        ],
                    },
                )
            except Exception:
                logger.exception(f"Failed to create feature flag {feature_name}")
        self._toggle(feature_name, flag)

    def save_many(self, flags: t.Dict[str, bool]) -> None:
        """Create many feature flags."""
        list(self.pool.map(lambda f: self.save(*f), flags.items()))

    def delete(self, feature_name: str) -> None:
        """Drop a feature flag."""
        logger.info(f"Deleting feature flag {feature_name}")
        requests.delete(
            f"https://app.harness.io/cf/admin/features/{feature_name}",
            headers={"x-api-key": self.api_key},
            params={
                "accountIdentifier": self.account,
                "orgIdentifier": self.organization,
                "projectIdentifier": self.project,
                "forceDelete": True,
            },
        )

    def delete_many(self, feature_names: t.List[str]) -> None:
        """Drop many feature flags."""
        list(self.pool.map(self.delete, feature_names))

    def apply_source(self, source: DltSource, *namespace: str) -> DltSource:
        """Apply the feature flags to a dlt source."""
        # NOTE: we use just the last section due to legacy design decisions
        # We will remove this when the Harness team cleans up the feature flag namespace
        ns = f"pipeline__{namespace[-1]}__{source.name}"

        # A closure to produce a resource id
        def _get_resource_id(resource: str) -> str:
            return f"{ns}__{resource}"

        resource_lookup = {
            _get_resource_id(key): resource
            for key, resource in source.resources.items()
        }
        every_resource = resource_lookup.keys()
        selected_resources = set(
            map(_get_resource_id, source.selected_resources.keys())
        )

        current_flags = set(
            filter(lambda f: f.startswith(ns), self.get_all_feature_names())
        )

        removed = current_flags.difference(every_resource)
        added = selected_resources.difference(current_flags)

        # TODO: reconciliation will be promoted to a top level context function
        if os.getenv("HARNESS_FF_AUTORECONCILE", "0") == "1":
            self.delete_many(list(removed))

        self.save_many({f: False for f in added})
        for f in added:
            resource_lookup[f].selected = False
        for f in current_flags.intersection(selected_resources):
            resource_lookup[f].selected = self.get(f).to_bool()

        return source

    def __del__(self) -> None:
        """Close the client."""
        if self._client is not None:
            self._client.close()
        if self._pool is not None:
            self._pool.shutdown()


__all__ = ["HarnessFeatureFlagAdapter"]
