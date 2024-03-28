"""Harness feature flag provider."""

import asyncio
import logging
import os
import typing as t
from concurrent.futures import ThreadPoolExecutor

import dlt
from dlt.common.configuration import with_config
from dlt.sources.helpers import requests
from featureflags.client import CfClient, Config, Target
from featureflags.evaluations.feature import FeatureConfigKind
from featureflags.interface import Cache
from featureflags.util import log as _ff_logger

import cdf.core.context as context
import cdf.core.logger as logger
from cdf.types.monads import Promise

if t.TYPE_CHECKING:
    from dlt.sources import DltSource

    from cdf.core.feature_flag import SupportsFFs


# This exists because the default harness LRU implementation does not store >1000 flags
# The interface is mostly satisfied by dict, so we subclass it and implement the missing methods
class _HarnessCache(dict, Cache):
    def set(self, key: str, value: bool) -> None:
        self[key] = value

    def remove(self, key: str | t.List[str]) -> None:
        if isinstance(key, str):
            self.pop(key, None)
        for k in key:
            self.pop(k, None)


@with_config(sections=("feature_flags", "options"))
def create_harness_provider(
    api_key: str = dlt.secrets.value,
    sdk_key: str = dlt.secrets.value,
    account: str = os.getenv("HARNESS_ACCOUNT_ID", dlt.config.value),
    organization: str = os.getenv("HARNESS_ORG_ID", dlt.config.value),
    project: str = os.getenv("HARNESS_PROJECT_ID", dlt.config.value),
) -> "SupportsFFs":
    _ff_logger.setLevel(logging.ERROR)

    def _get_client() -> CfClient:
        client = CfClient(
            sdk_key=sdk_key,
            config=Config(
                enable_stream=False, enable_analytics=False, cache=_HarnessCache()
            ),
        )
        client.wait_for_initialization()
        return client

    client = Promise(lambda: asyncio.to_thread(_get_client))

    def drop(ident: str) -> str:
        logger.info(f"Deleting feature flag {ident}")
        requests.delete(
            f"https://app.harness.io/gateway/cf/admin/features/{ident}",
            headers={"x-api-key": api_key},
            params={
                "accountIdentifier": account,
                "orgIdentifier": organization,
                "projectIdentifier": project,
                "forceDelete": True,
            },
        )
        return ident

    def create(ident: str, name: str) -> str:
        logger.info(f"Creating feature flag {ident}")
        requests.post(
            "https://app.harness.io/gateway/cf/admin/features",
            params={"accountIdentifier": account, "orgIdentifier": organization},
            headers={"Content-Type": "application/json", "x-api-key": api_key},
            json={
                "defaultOnVariation": "on-variation",
                "defaultOffVariation": "off-variation",
                "description": "Managed by CDF",
                "identifier": ident,
                "name": name,
                "kind": FeatureConfigKind.BOOLEAN.value,
                "permanent": True,
                "project": project,
                "variations": [
                    {"identifier": "on-variation", "value": "true"},
                    {"identifier": "off-variation", "value": "false"},
                ],
            },
        )
        return ident

    def _processor(source: "DltSource") -> "DltSource":
        nonlocal client
        workspace = context.active_workspace.get()
        if isinstance(client, Promise):
            client = client.unwrap()
        else:
            client._repository.cache.clear()
            client._polling_processor.retrieve_flags_and_segments()
        cache = client._repository.cache
        ns = f"pipeline__{workspace}__{source.name}"

        tpe = ThreadPoolExecutor(thread_name_prefix="harness-ff")

        def get_resource_id(r: str) -> str:
            return f"{ns}__{r}"

        resource_lookup = {
            get_resource_id(key): resource for key, resource in source.resources.items()
        }
        every_resource = resource_lookup.keys()
        selected_resources = set(map(get_resource_id, source.selected_resources.keys()))

        current_flags = set(
            filter(
                lambda f: f.startswith(ns),
                map(lambda f: f.split("/", 1)[1], cache.keys()),
            )
        )

        removed = current_flags.difference(every_resource)
        added = selected_resources.difference(current_flags)

        if os.getenv("HARNESS_FF_AUTORECONCILE", "0") == "1":
            list(tpe.map(drop, removed))
        for f in tpe.map(
            create,
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

    return _processor
