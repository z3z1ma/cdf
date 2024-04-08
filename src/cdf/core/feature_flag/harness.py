"""Harness feature flag provider."""

from __future__ import annotations

import asyncio
import logging
import os
import typing as t
from concurrent.futures import ThreadPoolExecutor

import pydantic
from dlt.sources import DltSource
from dlt.sources.helpers import requests
from featureflags.client import CfClient, Config, Target
from featureflags.evaluations.feature import FeatureConfigKind
from featureflags.interface import Cache
from featureflags.util import log as _ff_logger

import cdf.core.context as context
import cdf.core.logger as logger
from cdf.core.feature_flag.base import BaseFlagProvider
from cdf.types.monads import Promise


# This exists because the default harness LRU implementation does not store >1000 flags
# The interface is mostly satisfied by dict, so we subclass it and implement the missing methods
class _HarnessCache(dict, Cache):
    """A cache implementation for the harness feature flag provider."""

    def set(self, key: str, value: bool) -> None:
        self[key] = value

    def remove(self, key: str | t.List[str]) -> None:
        if isinstance(key, str):
            self.pop(key, None)
        for k in key:
            self.pop(k, None)


class HarnessFlagProvider(BaseFlagProvider, extra="allow"):
    """Harness feature flag provider."""

    api_key: str = pydantic.Field(
        description="The harness API key. Get it from your user settings.",
        pattern=r"^pat\.[a-zA-Z0-9_\-]+\.[a-fA-F0-9]+\.[a-zA-Z0-9_\-]+$",
    )
    sdk_key: pydantic.UUID4 = pydantic.Field(
        description="The harness SDK key. Get it from the environment management page of the FF module.",
    )
    account: str = pydantic.Field(
        os.getenv("HARNESS_ACCOUNT_ID", ...),
        description="The harness account ID.",
        min_length=22,
        max_length=22,
        pattern=r"^[a-zA-Z0-9_\-]+$",
    )
    organization: str = pydantic.Field(
        os.getenv("HARNESS_ORG_ID", ...),
        description="The harness organization ID.",
    )
    project: str = pydantic.Field(
        os.getenv("HARNESS_PROJECT_ID", ...),
        description="The harness project ID.",
    )

    provider: t.Literal["harness"] = pydantic.Field(
        "harness", frozen=True, description="The feature flag provider."
    )

    _client: t.Optional[CfClient] = None

    @pydantic.model_validator(mode="after")
    def _quiet_logger(self):
        """Configure the harness FF logger to only show errors. Its too verbose otherwise."""
        _ff_logger.setLevel(logging.ERROR)
        return self

    def _get_client(self) -> CfClient:
        """Get the client and cache it in the instance."""
        if self._client is not None:
            return self._client
        self._client = CfClient(
            sdk_key=str(self.sdk_key),
            config=Config(
                enable_stream=False, enable_analytics=False, cache=_HarnessCache()
            ),
        )
        self._client.wait_for_initialization()
        return self._client

    def drop(self, ident: str) -> str:
        """Drop a feature flag."""
        logger.info(f"Deleting feature flag {ident}")
        requests.delete(
            f"https://app.harness.io/gateway/cf/admin/features/{ident}",
            headers={"x-api-key": self.api_key},
            params={
                "accountIdentifier": self.account,
                "orgIdentifier": self.organization,
                "projectIdentifier": self.project,
                "forceDelete": True,
            },
        )
        return ident

    def create(self, ident: str, name: str) -> str:
        """Create a feature flag."""
        logger.info(f"Creating feature flag {ident}")
        try:
            requests.post(
                "https://app.harness.io/gateway/cf/admin/features",
                params={
                    "accountIdentifier": self.account,
                    "orgIdentifier": self.organization,
                },
                headers={"Content-Type": "application/json", "x-api-key": self.api_key},
                json={
                    "defaultOnVariation": "on-variation",
                    "defaultOffVariation": "off-variation",
                    "description": "Managed by CDF",
                    "identifier": ident,
                    "name": name,
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
            logger.exception(f"Failed to create feature flag {ident}")
        return ident

    def apply_source(self, source: DltSource) -> DltSource:
        """Apply the feature flags to a dlt source."""
        client = Promise(lambda: asyncio.to_thread(self._get_client))
        workspace = context.active_project.get()
        if isinstance(client, Promise):
            client = client.unwrap()
        else:
            client._repository.cache.clear()
            client._polling_processor.retrieve_flags_and_segments()
        cache = client._repository.cache
        tpe = ThreadPoolExecutor(thread_name_prefix="harness-ff")
        namespace = f"pipeline__{workspace.name}__{source.name}"

        def get_resource_id(r: str) -> str:
            return f"{namespace}__{r}"

        resource_lookup = {
            get_resource_id(key): resource for key, resource in source.resources.items()
        }
        every_resource = resource_lookup.keys()
        selected_resources = set(map(get_resource_id, source.selected_resources.keys()))

        current_flags = set(
            filter(
                lambda f: f.startswith(namespace),
                map(lambda f: f.split("/", 1)[1], cache.keys()),
            )
        )

        removed = current_flags.difference(every_resource)
        added = selected_resources.difference(current_flags)

        if os.getenv("HARNESS_FF_AUTORECONCILE", "0") == "1":
            list(tpe.map(self.drop, removed))
        for f in tpe.map(
            self.create,
            added,
            [
                f"Extract {source.name.title()} {resource_lookup[f].name.title()}"
                for f in added
            ],
        ):
            resource_lookup[f].selected = False
        for f in current_flags.intersection(selected_resources):
            resource_lookup[f].selected = client.bool_variation(f, Target("cdf"), False)

        return source


__all__ = ["HarnessFlagProvider"]
