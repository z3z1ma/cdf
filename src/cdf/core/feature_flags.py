"""Feature flags for CDF."""
import abc
import json
import logging
import typing as t
from concurrent.futures import ThreadPoolExecutor
from functools import lru_cache
from threading import Lock

import dlt
from dlt.common.configuration import with_config
from dlt.sources import DltSource
from dlt.sources.helpers import requests
from featureflags.client import CfClient, Config, Target
from featureflags.evaluations.feature import FeatureConfigKind
from featureflags.interface import Cache
from featureflags.util import log as _ff_logger

import cdf.core.constants as c
import cdf.core.logger as logger

if t.TYPE_CHECKING:
    from cdf.core.workspace import Workspace


# The goal is to keep imports deferred here for performance reasons.
# The only place this will be imported is in wrapper/header functions.
# Which means this is injected into user code and evaluated when that is evaluated.
# So the NEW interface should receive:
# - a source object, which will be a DltSource
# -
#
#
# Q: workspaces need a persistent label that isnt dependent on the parent directory
# Q: we need to pass this name to the FF provider? Rather the whole workspace
# since the local provider needs the root path of the ws while others need the name?
# A: We can technically rely on dir name IF we are in a multi-workspace layout (nested dirs)
# since subdir names are unique. But we can't rely on that in a single workspace layout.


class SupportsFFs(t.Protocol):
    def __call__(self, source: DltSource) -> DltSource:
        ...


Provider = t.Literal["local", "harness", "launchdarkly"]


class AbstractFeatureFlagProvider(abc.ABC):
    """Base class for feature flag providers"""

    def __init__(self, workspace: "Workspace"):
        self.workspace = workspace

    @abc.abstractmethod
    def exists(self, identifier: str) -> bool:
        """Check if a flag exists.

        Args:
            identifier (str): The unique identifier for the flag

        Returns:
            bool: True if the flag exists, False otherwise
        """
        raise NotImplementedError

    @abc.abstractmethod
    def get_one(self, identifier: str) -> bool:
        """Get a flag.

        Args:
            identifier (str): The unique identifier for the flag

        Returns:
            bool: True if the flag is enabled, False otherwise
        """
        raise NotImplementedError

    def get_many(self, identifiers: t.List[str]) -> t.List[bool]:
        """Get many flags.

        Args:
            identifiers (list[str]): A list of flag identifiers

        Returns:
            dict: A mapping of flag identifiers to enabled status
        """
        return [self.get_one(i) for i in identifiers]

    @abc.abstractmethod
    def create_one(
        self, identifier: str, display_name: str | None = None, **kwargs: t.Any
    ) -> None:
        """Create a flag.

        Args:
            identifier (str): A unique identifier for the flag
            display_name (str): The display name for the flag which may be leveraged by the provider
            **kwargs: Additional keyword arguments to pass to the implementation
        """
        raise NotImplementedError

    def create_many(self, mappings: t.Dict[str, str], **kwargs: t.Any) -> None:
        """Create many flags.

        Args:
            mappings (dict): A mapping of flag identifiers to display names
        """
        for k, v in mappings.items():
            self.create_one(k, v, **kwargs)

    @abc.abstractmethod
    def drop_one(self, identifier: str) -> None:
        """Drop a flag.

        Args:
            identifier (str): The unique identifier for the flag
        """
        raise NotImplementedError

    def drop_many(self, identifiers: t.List[str]) -> None:
        """Drop many flags.

        Args:
            identifiers (list[str]): A list of flag identifiers
        """
        for i in identifiers:
            self.drop_one(i)


class LocalFeatureFlagProvider(AbstractFeatureFlagProvider):
    _MUTEX = Lock()

    def exists(self, identifier: str) -> bool:
        """Check if a flag exists.

        Args:
            identifier (str): The unique identifier for the flag

        Returns:
            bool: True if the flag exists, False otherwise
        """
        return identifier in LocalFeatureFlagProvider.read_from_disk(self.workspace)

    def get_one(self, identifier: str) -> bool:
        """Get a flag.

        Args:
            identifier (str): The unique identifier for the flag

        Returns:
            bool: True if the flag is enabled, False otherwise
        """
        return LocalFeatureFlagProvider.read_from_disk(self.workspace).get(
            identifier, False
        )

    def create_one(
        self, identifier: str, display_name: str | None = None, **kwargs: t.Any
    ) -> None:
        """Create a flag.

        Args:
            identifier (str): A unique identifier for the flag
            display_name (str): The display name for the flag which may be leveraged by the provider
            **kwargs: Additional keyword arguments to pass to the implementation
        """
        with LocalFeatureFlagProvider._MUTEX:
            existing_flags = LocalFeatureFlagProvider.read_from_disk(self.workspace)
            existing_flags[identifier] = False
            self.workspace.root.joinpath(c.FLAG_FILE).write_text(
                json.dumps(existing_flags, indent=2)
            )

    def drop_one(self, identifier: str) -> None:
        """Drop a flag.

        Args:
            identifier (str): The unique identifier for the flag
        """
        with LocalFeatureFlagProvider._MUTEX:
            existing_flags = LocalFeatureFlagProvider.read_from_disk(self.workspace)
            existing_flags.pop(identifier, None)
            self.workspace.root.joinpath(c.FLAG_FILE).write_text(
                json.dumps(existing_flags, indent=2)
            )

    @lru_cache(maxsize=10)
    @staticmethod
    def read_from_disk(workspace: "Workspace") -> t.Dict[str, bool]:
        """Get flags for a workspace.

        Args:
            workspace (Workspace): The workspace to get flags for

        Returns:
            dict: A mapping of flag identifiers to enabled status
        """
        logger.debug("Searching for flags in %s", workspace.root)

        feature_flags = {}
        f = workspace.root / c.FLAG_FILE

        if f.exists():
            try:
                feature_flags.update(json.loads(f.read_bytes()))
            except json.JSONDecodeError as err:
                logger.warning("Failed to parse flags in %s: %s", f, err)

        return feature_flags


class HarnessFeatureFlagProvider(AbstractFeatureFlagProvider):
    class _Cache(dict, Cache):
        """This exists because the default harness LRU impl sucks and does not work with >1000 flags"""

        def set(self, key: str, value: bool) -> None:
            """Set a flag in the cache."""
            self[key] = value

        def remove(self, key: str | t.List[str]) -> None:
            """Remove a flag from the cache."""
            if isinstance(key, str):
                self.pop(key, None)
            for k in key:
                self.pop(k, None)

    @with_config(sections=("ff", "harness"))
    def __init__(
        self,
        workspace: "Workspace",
        api_key: str = dlt.secrets.value,
        sdk_key: str = dlt.secrets.value,
        account: str = dlt.config.value,
        organization: str = dlt.config.value,
        project: str = dlt.config.value,
        environment: str = dlt.config.value,
    ):
        super().__init__(workspace)
        self.api_key = api_key
        self.sdk_key = sdk_key
        self.account = account
        self.organization = organization
        self.project = project
        self.environment = environment
        _ff_logger.setLevel(logging.ERROR)
        self.client = HarnessFeatureFlagProvider.get_client(sdk_key)

    @lru_cache(maxsize=1)
    @staticmethod
    def get_client(sdk_key: str = dlt.secrets.value) -> CfClient:
        """Get a harness client. This is cached so we don't have to create a new client.

        Args:
            sdk_key: The sdk key to use to authenticate with harness.

        Returns:
            The client.
        """
        client = CfClient(
            sdk_key=sdk_key,
            config=Config(
                enable_stream=False,
                enable_analytics=False,
                cache=HarnessFeatureFlagProvider._Cache(),
            ),
        )
        client.wait_for_initialization()
        return client

    def exists(self, identifier: str) -> bool:
        """Check if a flag exists in the Harness Platform API

        We make this fast by checking the SDK cache.

        Args:
            identifier (str): The identifier for the flag

        Returns:
            bool: True if the flag exists, False otherwise
        """
        return "flags/" + self._cdf_id_to_harness_id(identifier) in list(
            self.client._repository.cache.keys()
        )

    def get_one(self, identifier: str) -> bool:
        """Get a flag from the harness feature flag sdk client

        Args:
            identifier (str): The flag identifier

        Returns:
            bool: True if the flag is enabled, False otherwise
        """
        return self.client.bool_variation(
            self._cdf_id_to_harness_id(identifier), Target("cdf"), False
        )

    def create_one(
        self,
        identifier: str,
        name: str,
        description: str = "Toggle to extract this resource - managed by cdf",
        **kwargs: t.Any,
    ) -> None:
        """Create a flag in the harness platform api

        Args:
            identifier (str): The identifier for the flag
            name (str): The name for the flag
            **kwargs: Additional keyword arguments to pass to the Harness Platform API

        Raises:
            HTTPError: If the response from the Harness Platform API is not successful
        """
        resp = requests.post(
            "https://app.harness.io/gateway/cf/admin/features",
            params={
                "accountIdentifier": self.account,
                "orgIdentifier": self.organization,
            },
            headers={"Content-Type": "application/json", "x-api-key": self.api_key},
            json={
                "defaultOnVariation": "on-variation",
                "defaultOffVariation": "off-variation",
                "description": description,
                "identifier": self._cdf_id_to_harness_id(identifier),
                "name": name,
                "kind": FeatureConfigKind.BOOLEAN.value,
                "permanent": True,
                "project": self.project,
                "variations": [
                    {
                        "identifier": "on-variation",
                        "value": "true",
                    },
                    {
                        "identifier": "off-variation",
                        "value": "false",
                    },
                ],
                **kwargs,
            },
        )
        resp.raise_for_status()
        return resp.json()

    def drop_one(self, identifier: str) -> None:
        """Drop a flag from the Harness Platform API

        Args:
            identifier (str): The identifier for the flag

        Raises:
            HTTPError: If the request fails
        """
        resp = requests.delete(
            f"https://app.harness.io/gateway/cf/admin/features/{identifier}",
            headers={"x-api-key": self.api_key},
            params={
                "accountIdentifier": self.account,
                "orgIdentifier": self.organization,
                "projectIdentifier": self.project,
                "forceDelete": True,
            },
        )
        resp.raise_for_status()
        self.client._repository.cache.remove([f"flags/{identifier}"])
        self.client._polling_processor.retrieve_flags_and_segments()

    def drop_many(self, identifiers: t.List[str]) -> None:
        """Drop many flags from the Harness Platform API

        Args:
            identifiers (list[str]): A list of flag identifiers

        Raises:
            HTTPError: If the request fails
        """

        def _drop(i: str) -> None:
            resp = requests.delete(
                f"https://app.harness.io/gateway/cf/admin/features/{i}",
                headers={"x-api-key": self.api_key},
                params={
                    "accountIdentifier": self.account,
                    "orgIdentifier": self.organization,
                    "projectIdentifier": self.project,
                    "forceDelete": True,
                },
            )
            resp.raise_for_status()
            self.client._repository.cache.remove([f"flags/{i}"])

        with ThreadPoolExecutor() as executor:
            executor.map(_drop, identifiers)
        executor.shutdown(wait=True)

        self.client._polling_processor.retrieve_flags_and_segments()

    @staticmethod
    def _harness_id_to_cdf_id(harness_id: str) -> str:
        """Convert a harness platform api flag id to a cdf flag id

        Example:
            flags/pipeline__datateam__sfdc__account -> pipeline:datateam.sfdc:account

        Args:
            harness_id (str): The Harness Platform API flag identifier

        Returns:
            str: The cdf flag identifier
        """
        if harness_id.upper().startswith("FLAGS/"):
            harness_id = harness_id.split("/", 1)[1]

        typ, workspace, component, subcomponent = harness_id.split("__", 3)
        return f"{typ}:{workspace}.{component}:{subcomponent}"

    @staticmethod
    def _cdf_id_to_harness_id(cdf_id: str) -> str:
        """Convert a cdf flag id to a harness platform api flag id

        Example:
            pipeline:datateam.sfdc:account -> pipeline__datateam__sfdc__account

        Args:
            cdf_id (str): The cdf flag identifier

        Returns:
            str: The Harness Platform API flag identifier
        """
        typ, workspace_component, subcomponent = cdf_id.split(":", 2)
        workspace, component = workspace_component.split(".", 1)
        return f"{typ}__{workspace}__{component}__{subcomponent}"


class LaunchDarklyFeatureFlagProvider(AbstractFeatureFlagProvider):
    ...


class SupportsFeatureFlags(t.Protocol):
    """Bare minimum interface to integrate feature flags with a workspace component"""

    workspace: "Workspace"

    def create_one(
        self, identifier: str, display_name: str | None = None, **kwargs: t.Any
    ) -> None:
        ...

    def exists(self, identifier: str) -> bool:
        ...

    def get_one(self, identifier: str) -> bool:
        ...


def process_source(source: DltSource, provider: SupportsFeatureFlags) -> DltSource:
    def _get_id(r: str) -> str:
        return f"pipeline:{provider.workspace.name}.{source.name}:{r}"

    resources = source.resources.keys()
    for r in resources:
        id_ = _get_id(r)
        if not provider.exists(id_):
            provider.create_one(id_, f"Extract {source.name.title()} {r.title()}")
            source.resources[r].selected = False
        else:
            source.resources[r].selected = provider.get_one(id_)
    return source


@with_config(sections=("ff",))
def get_provider(
    workspace: "Workspace", provider: Provider | None = None
) -> AbstractFeatureFlagProvider:
    """Get a feature flag provider.

    Args:
        workspace (Workspace): The workspace to get the provider for
        provider (Provider): The provider to use, if None will use the configured provider

    Returns:
        BaseFeatureFlagProvider: The provider
    """

    if provider == "local" or provider is None:
        return LocalFeatureFlagProvider(workspace)
    elif provider == "harness":
        return HarnessFeatureFlagProvider(workspace)
    elif provider == "launchdarkly":
        return LaunchDarklyFeatureFlagProvider(workspace)  # type: ignore
    else:
        raise ValueError(
            f"Invalid provider: {provider}, must be one of {t.get_args(Provider)}"
        )
