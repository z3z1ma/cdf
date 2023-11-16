"""Feature flags for CDF.

NOTE: our primary usage pattern of FF is to get a bunch of flags
for a single component, so we should optimize for that
This means we may not need to pull every possible flag into
the cache. Thus our main entrypoint should be something like
get_component_ff(component_id: str, populate_cache_fn=get_or_create_flag_dispatch)

component_id = <source|transform|publisher>:<name>
flag_name = <component_id>:<flag_name>
"""
import json
import logging
import typing as t
from functools import lru_cache
from hashlib import sha256
from threading import Lock

import dlt
from dlt.extract.source import DltSource
from dlt.sources.helpers import requests
from featureflags.client import CfClient, Config, Target
from featureflags.evaluations.feature import FeatureConfigKind
from featureflags.interface import Cache
from featureflags.util import log as _ff_logger

import cdf.core.constants as c
import cdf.core.logger as logger
from cdf.core.config import populate_fn_kwargs_from_config
from cdf.core.monads import Result
from cdf.core.utils import get_source_component_id, qualify_source_component_id

if t.TYPE_CHECKING:
    from cdf.core.workspace import Workspace

FlagDict = t.Dict[str, bool]
SourceFlagDict = t.Dict[DltSource, FlagDict]

Provider = t.Literal["local", "harness", "launchdarkly"]


class FnPopulateCache(t.Protocol):
    def __call__(
        self,
        cache: FlagDict | None,
        source: DltSource,
        workspace: "Workspace",
        *kwargs: t.Any,
    ) -> FlagDict:
        ...


CACHE: SourceFlagDict = {}
_LOCAL_CACHE_MUTEX = Lock()


class _Cache(dict, Cache):
    """This exists because the default harness LRU impl sucks and does not work with >1000 flags"""

    def set(self, key: str, value: bool) -> None:
        """Set a flag in the cache."""
        self[key] = value

    def remove(self, key) -> None:
        """Remove a flag from the cache."""
        self.pop(key, None)


@lru_cache(maxsize=1)
def _get_harness_client(sdk_key: str) -> CfClient:
    """Get a harness client. This is cached so we don't have to create a new client.

    Args:
        sdk_key: The sdk key to use to authenticate with harness.

    Returns:
        The client.
    """
    client = CfClient(
        sdk_key=sdk_key,
        config=Config(enable_stream=False, enable_analytics=False, cache=_Cache()),
    )
    client.wait_for_initialization()
    return client


def _create_harness_flag(
    identifier: str,
    name: str,
    api_key: str,
    account: str,
    organization: str,
    project: str,
    description: str = "Toggle to extract this resource - managed by cdf",
    **kwargs: t.Any,
) -> Result[dict]:
    """Create a flag in the Harness Platform API

    Args:
        identifier (str): The identifier for the flag
        name (str): The name for the flag
        **kwargs: Additional keyword arguments to pass to the Harness Platform API

    Returns:
        dict: The response from the Harness Platform API

    Raises:
        AssertionError: If the number of variations is less than two
        requests.exceptions.HTTPError: If the response from the Harness Platform API
            is not successful
    """
    variations = [
        {
            "identifier": "on-variation",
            "value": "true",
        },
        {
            "identifier": "off-variation",
            "value": "false",
        },
    ]
    resp = requests.post(
        "https://app.harness.io/gateway/cf/admin/features",
        params={
            "account": account,
            "organization": organization,
        },
        headers={
            "Content-Type": "application/json",
            "x-api-key": api_key,
        },
        json={
            "defaultOnVariation": variations[0]["identifier"],
            "defaultOffVariation": variations[1]["identifier"],
            "description": description,
            "identifier": identifier,
            "name": name,
            "kind": FeatureConfigKind.BOOLEAN.value,
            "permanent": True,
            "project": project,
            "variations": variations,
            **kwargs,
        },
    )
    if resp.ok:
        return Result(resp.json(), None)
    return Result(None, RuntimeError(resp.text))


@lru_cache(maxsize=1)
def _get_harness_flag(
    identifier: str,
    api_key: str,
    account: str,
    organization: str,
    project: str,
    environment: str,
) -> Result[dict]:
    """Get a flag from the Harness Platform API

    Args:
        identifier (str): The flag identifier

    Returns:
        dict: The flag

    Raises:
        requests.exceptions.HTTPError: If the request fails
    """
    resp = requests.get(
        f"https://app.harness.io/gateway/cf/admin/features/{identifier}",
        headers={"x-api-key": api_key},
        params={
            "account": account,
            "organization": organization,
            "project": project,
            "environment": environment,
            "metrics": False,
        },
    )
    if resp.ok:
        return Result(resp.json(), None)
    return Result(None, Exception(resp.text))


def _harness_flag_exists(identifier: str, ff_client: CfClient) -> bool:
    """Check if a flag exists in the Harness Platform API

    We make this fast by checking the SDK cache.

    Args:
        identifier (str): The identifier for the flag

    Returns:
        bool: True if the flag exists, False otherwise
    """
    try:
        return f"flags/{identifier}" in list(
            ff_client._repository.cache.keys()
        ) or bool(Result.apply(_get_harness_flag, identifier))
    except requests.HTTPError as e:
        if e.response and e.response.status_code in (404, 400):
            return False
        raise


def _harness_id_to_component_id(harness_id: str) -> str:
    """Convert a Harness Platform API flag id to a component id

    Example:
        workspace_X_source__resource -> source:workspace.source:resource

    Args:
        harness_id (str): The Harness Platform API flag id

    Returns:
        str: The component id
    """
    if harness_id.upper().startswith("FLAGS/"):
        harness_id = harness_id.split("/", 1)[1]
    return "source:" + harness_id.replace("_X_", ".", 1).replace("__", ":", 1)


def _component_id_to_harness_id(component_id: str) -> str:
    """Convert a component id to a Harness Platform API flag id

    Example:
        source:workspace.source:resource -> workspace_X_source__resource

    Args:
        component_id (str): The component id

    Returns:
        str: The Harness Platform API flag id
    """
    return "__".join(component_id.split(":")[1:]).replace(".", "_X_")


def get_or_create_flag_harness(
    cache: FlagDict | None,
    /,
    source: DltSource,
    workspace: "Workspace",
    *,
    account: str,
    project: str,
    organization: str,
    sdk_key: str,
    api_key: str,
) -> t.Tuple[FlagDict, dict]:
    """Populate a cache with flags.

    Args:
        cache: A cache to populate.
        source: The DltSource to get flags for.
        workspace: The workspace which we are getting flags for.
        account: The harness account id.
        project: The harness project id.
        organization: The harness organization id.
        sdk_key: The sdk key to use with Harness FF. This is specific to an env in Harness.
        api_key: The api key to use to authenticate with harness. This is used to create flags
            via the platform api which is separate from the FF SDK.

    Returns:
        dict: The populated cache.
    """
    _ff_logger.setLevel(logging.ERROR)  # harness is so noisy for our use case
    cache = cache if cache is not None else CACHE.setdefault(source, {})
    ff_client = _get_harness_client(sdk_key or dlt.secrets["ff.harness.sdk_key"])
    cache.update(
        {
            _harness_id_to_component_id(k): ff_client.bool_variation(
                k[6:], Target("cdf"), False
            )
            for k in ff_client._repository.cache.keys()
        }
    )
    for resource in source.resources.keys():
        component = get_source_component_id(source, resource, workspace.namespace)
        if component in cache:
            continue
        harness_safeident = _component_id_to_harness_id(component)
        exists = _harness_flag_exists(harness_safeident, ff_client)
        if not exists:
            _create_harness_flag(
                harness_safeident,
                f"Extract {' '.join(component.split(':')[1:]).title()}",
                api_key=api_key,
                account=account,
                organization=organization,
                project=project,
            )
            cache[component] = False
        else:
            rv = ff_client.bool_variation(harness_safeident, Target("cdf"), False)
            cache[component] = rv
    # TODO: the cache key should prevent us from using the wrong config but expired tokens
    # will require git commits since the lockfile is versioned
    return cache, {"cache_key": sha256(sdk_key.encode()).hexdigest()}


def get_or_create_flag_launchdarkly(
    cache: FlagDict | None,
    /,
    source: DltSource,
    workspace: "Workspace",
    *,
    account: str | None = None,
    api_key: str | None = None,
) -> t.Tuple[FlagDict, dict]:
    """Populate a cache with flags. This is not implemented.

    Args:
        cache: A cache to populate.
        source: The DltSource to get flags for.
        workspace: The workspace which we are getting flags for.
        account: The launchdarkly account id.
        api_key: The api key to use to authenticate with launchdarkly.

    Returns:
        dict: The populated cache.
    """
    _ = cache, source, workspace, account, api_key
    raise NotImplementedError


def _write_local_flag_file(
    cache: FlagDict,
    source: DltSource,
    workspace: "Workspace",
) -> None:
    """Write a flag file to the workspace.

    Args:
        cache: The populated cache representing existing flags.
        source: The DltSource to diff against.
        workspace: The workspace which we are operating in.
    """
    with _LOCAL_CACHE_MUTEX:
        discovered = {}
        for resource in source.resources.keys():
            component = get_source_component_id(source, resource, workspace.namespace)
            if component not in cache:
                discovered[component] = False
        if discovered:
            fpath = workspace.root / c.FLAG_FILES[0]
            logger.debug("Updating flags in %s", fpath)
            if fpath.exists():
                known = json.loads(fpath.read_text())
                discovered.update(known)
            fpath.write_text(json.dumps(discovered, indent=2))


def get_or_create_flag_local(
    cache: FlagDict | None,
    /,
    source: DltSource,
    workspace: "Workspace",
) -> t.Tuple[FlagDict, dict]:
    """Populate a cache with flags.

    Args:
        cache: A cache to populate.
        source: The DltSource to get flags for.
        workspace: The workspace which we are getting flags for.

    Returns:
        dict: The populated cache.
    """
    cache = cache if cache is not None else CACHE.setdefault(source, {})

    for path in (
        workspace.root,
        workspace.root / c.SOURCES_PATH,
    ):
        logger.debug("Searching for flags in %s", path)
        flags = {}

        # Find flags
        for f in c.FLAG_FILES:
            fp = path / f
            if not fp.exists():
                continue
            try:
                flags.update(json.loads(fp.read_text()))
            except json.JSONDecodeError as e:
                logger.warning("Failed to parse flags in %s: %s", fp, e)

        # Qualify flags as needed
        for component_id in list(flags.keys()):
            qualified_component_id = qualify_source_component_id(
                component_id, workspace.namespace
            )
            if qualified_component_id != component_id:
                logger.debug(
                    "Found flag %s, qualified as %s",
                    component_id,
                    qualified_component_id,
                )
                flags[qualified_component_id] = flags.pop(component_id)

        logger.debug("Found flags: %s", flags)
        cache.update(flags)

    # Write new flags to flag file
    _write_local_flag_file(cache, source, workspace)

    return cache, {}


def toggle_flag_dispatch() -> Result:
    raise NotImplementedError


def delete_flag_dispatch() -> Result:
    raise NotImplementedError


def get_or_create_flag_dispatch(
    cache: FlagDict | None,
    /,
    source: DltSource,
    workspace: "Workspace",
    provider: Provider | None = None,
    **kwargs: t.Any,
) -> t.Tuple[FlagDict, dict]:
    """Populate a cache with flags.

    This function dispatches to the appropriate implementation based on the
    provider specified in the config.

    Args:
        cache: A cache to populate.
        source: The DltSource to get flags for. The implementation should use this information
            to scope requests to the feature flag provider when possible.
        workspace: The workspace which we are getting flags for. The implementation should use this information
            to ensure flags are namespaced to the workspace. Two workspaces can have the same source names.
        provider: The provider to use. If falsey, the provider from the config is used. If there is no
            resolved provider, the local provider is used.
        **kwargs: Additional keyword arguments to pass to the implementation. These will be supplemented
            by the dlt config providers.

    Returns:
        dict: The populated cache.
    """
    cache = cache if cache is not None else CACHE.setdefault(source, {})

    try:
        provider = provider or dlt.config["ff.provider"]
    except KeyError:
        provider = "local"

    if provider == "local":
        fn = get_or_create_flag_local
    elif provider == "harness":
        fn = get_or_create_flag_harness
    elif provider == "launchdarkly":
        fn = get_or_create_flag_launchdarkly
    else:
        raise ValueError(
            f"Invalid provider: {provider}, must be one of {t.get_args(Provider)}"
        )

    kwargs = populate_fn_kwargs_from_config(
        fn,
        kwargs,
        private_attrs={"cache", "source", "workspace"},
        config_path=["ff", provider],
    )
    return fn(cache, source, workspace, **kwargs)


def apply_feature_flags(
    source: DltSource,
    flags: t.Dict[str, bool],
    workspace: str | None = None,
    raise_on_no_resources: bool = False,
) -> DltSource:
    """Apply feature flags to a source."""

    logger.debug("Applying feature flags for source %s", source.name)
    for resource in source.resources.values():
        key = get_source_component_id(source, resource, workspace)
        fv = flags.get(key)
        if fv is None:
            logger.debug("No flag for %s", key)
            fv = False
        elif fv is False:
            logger.debug("Flag for %s is False", key)
        elif fv is True:
            logger.debug("Flag for %s is True", key)
        resource.selected = fv

    if raise_on_no_resources and not source.resources.selected:
        raise ValueError(f"No resources selected for source {source.name}")

    return source
