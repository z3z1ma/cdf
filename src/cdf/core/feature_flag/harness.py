"""Harness feature flag provider."""

from __future__ import annotations

import asyncio
import logging
import os
import typing as t
from concurrent.futures import ThreadPoolExecutor
from functools import lru_cache

from dlt.sources import DltSource
from dlt.sources.helpers import requests
from featureflags.client import CfClient, Config, Target
from featureflags.evaluations.feature import FeatureConfigKind
from featureflags.interface import Cache
from featureflags.util import log as _ff_logger

import cdf.core.context as context
import cdf.core.logger as logger
from cdf.types.monads import Promise

if t.TYPE_CHECKING:
    from cdf.core.project import HarnessFeatureFlagSettings


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


def _quiet_logger():
    """Configure the harness FF logger to only show errors. Its too verbose otherwise."""
    _ff_logger.setLevel(logging.ERROR)


@lru_cache(maxsize=2)
def _get_client(settings: "HarnessFeatureFlagSettings") -> CfClient:
    """Get the client and cache it in the instance."""
    _quiet_logger()
    client = CfClient(
        sdk_key=str(settings.sdk_key),
        config=Config(
            enable_stream=False, enable_analytics=False, cache=_HarnessCache()
        ),
    )
    client.wait_for_initialization()
    return client


def drop(identifier: str, settings: "HarnessFeatureFlagSettings") -> str:
    """Drop a feature flag."""
    logger.info(f"Deleting feature flag {identifier}")
    requests.delete(
        f"https://app.harness.io/cf/admin/features/{identifier}",
        headers={"x-api-key": settings.api_key},
        params={
            "accountIdentifier": settings.account,
            "orgIdentifier": settings.organization,
            "projectIdentifier": settings.project,
            "forceDelete": True,
        },
    )
    return identifier


def create(identifier: str, name: str, settings: "HarnessFeatureFlagSettings") -> str:
    """Create a feature flag."""
    logger.info(f"Creating feature flag {identifier}")
    try:
        requests.post(
            "https://app.harness.io/cf/admin/features",
            params={
                "accountIdentifier": settings.account,
                "orgIdentifier": settings.organization,
            },
            headers={"Content-Type": "application/json", "x-api-key": settings.api_key},
            json={
                "defaultOnVariation": "on-variation",
                "defaultOffVariation": "off-variation",
                "description": "Managed by CDF",
                "identifier": identifier,
                "name": name,
                "kind": FeatureConfigKind.BOOLEAN.value,
                "permanent": True,
                "project": settings.project,
                "variations": [
                    {"identifier": "on-variation", "value": "true"},
                    {"identifier": "off-variation", "value": "false"},
                ],
            },
        )
    except Exception:
        logger.exception(f"Failed to create feature flag {identifier}")
    return identifier


def apply_source(
    source: DltSource, /, *, settings: "HarnessFeatureFlagSettings", **kwargs: t.Any
) -> DltSource:
    """Apply the feature flags to a dlt source."""
    _ = kwargs
    client = Promise(lambda: asyncio.to_thread(_get_client, settings))
    # TODO: can we get rid of this?
    workspace = context.active_workspace.get()
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
        list(tpe.map(drop, removed, [settings] * len(removed)))
    for f in tpe.map(
        create,
        added,
        [
            f"Extract {source.name.title()} {resource_lookup[f].name.title()}"
            for f in added
        ],
        [settings] * len(added),
    ):
        resource_lookup[f].selected = False
    for f in current_flags.intersection(selected_resources):
        resource_lookup[f].selected = client.bool_variation(f, Target("cdf"), False)

    return source


__all__ = ["apply_source"]
