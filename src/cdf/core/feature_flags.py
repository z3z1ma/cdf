"""Feature flags for CDF.

NOTE: our primary usage pattern of FF is to get a bunch of flags
for a single component, so we should optimize for that
This means we may not need to pull every possible flag into
the cache. Thus our main entrypoint should be something like
get_flags_for_component(component_id: str, populate_cache_fn=populate_flag_cache)

component_id = <source|transform|publisher>:<name>
flag_name = <component_id>:<flag_name>
"""
import json
import typing as t
from functools import partial
from pathlib import Path

import dlt

import cdf.core.constants as c
from cdf.core.utils import do, search_merge_json

TFlag = t.Union[str, bool, float, t.Dict[str, str]]
TFlags = t.Dict[str, TFlag]

FLAGS: TFlags = {}

Providers = t.Literal["local", "harness", "launchdarkly"]


def populate_flag_cache_from_config(
    cache: dict[str, TFlag] | None = None,
    /,
    component_id: str | None = None,
    *,
    with_provider: Providers | None = None,
    **kwargs: t.Any,
) -> TFlags:
    """Populate a cache with flags.

    This function dispatches to the appropriate implementation based on the
    provider specified in the config.
    """
    provider: Providers = with_provider or dlt.config["feature_flags.provider"]
    if provider == "local":
        merged_kwargs = {**dict(component_id=component_id), **kwargs}
        return populate_flag_cache_from_local(cache, **merged_kwargs)
    elif provider == "harness":
        merged_kwargs = {
            **dict(
                component_id=component_id,
                account_id=dlt.config["feature_flags.harness.account_id"],
                project_id=dlt.config["feature_flags.harness.project_id"],
                org_id=dlt.config["feature_flags.harness.org_id"],
                api_key=dlt.secrets["feature_flags.harness.api_key"],
            ),
            **kwargs,
        }
        return populate_flag_cache_from_harness(cache, **merged_kwargs)
    elif provider == "launchdarkly":
        merged_kwargs = {
            **dict(
                component_id=component_id,
                account_id=dlt.config["feature_flags.launchdarkly.account_id"],
                api_key=dlt.secrets["feature_flags.launchdarkly.api_key"],
            ),
            **kwargs,
        }
        return populate_flag_cache_from_launchdarkly(cache, **merged_kwargs)
    else:
        raise ValueError(
            f"Invalid provider: {provider}, must be one of {t.get_args(Providers)}"
        )


def populate_flag_cache_from_harness(
    cache: dict[str, TFlag] | None = None,
    /,
    component_id: str | None = None,
    account_id: str | None = None,
    project_id: str | None = None,
    org_id: str | None = None,
    api_key: str | None = None,
) -> TFlags:
    cache = cache if cache is not None else {}
    if not (account_id and project_id and org_id and api_key):
        raise ValueError(
            "Must supply account_id, project_id, org_id, and api_key to use harness provider"
        )
    return cache


def populate_flag_cache_from_launchdarkly(
    cache: dict[str, TFlag] | None = None,
    /,
    component_id: str | None = None,
    account_id: str | None = None,
    api_key: str | None = None,
) -> TFlags:
    cache = cache if cache is not None else {}
    if not (account_id and api_key):
        raise ValueError(
            "Must supply account_id and api_key to use launchdarkly provider"
        )
    return cache


def populate_flag_cache_from_local(
    cache: dict[str, TFlag] | None = None,
    /,
    component_id: str | None = None,
    component_paths: t.Iterable[str | Path] | None = None,
    max_depth: int = 3,
) -> TFlags:
    """Populate a cache with flags.

    Args:
        cache: A cache to populate.
        component_id: The id of the component to search for flags. Used to filter the cache.
            This is not used for the local implementation but is a required parameter for
            the interface.
        component_paths: A list of paths to search for flags. Supplied via closure.

    Returns:
        dict: The populated cache.
    """
    _ = component_id  # This is cheap, we don't need the id
    cache = cache if cache is not None else {}
    component_paths = component_paths or c.COMPONENT_PATHS
    for raw_path in component_paths:
        path = Path(raw_path).expanduser().resolve()
        search_parent_dirs = path != Path.home()
        if search_parent_dirs:
            do(
                cache.update,
                map(lambda f: search_merge_json(path, f, max_depth), c.CDF_FLAG_FILES),
            )
        else:
            do(
                cache.update,
                map(
                    lambda f: json.loads((path / f).read_text()),
                    filter(lambda f: (path / f).exists(), c.CDF_FLAG_FILES),
                ),
            )
    return cache


def get_flags_for_component(
    component_id: str,
    populate_cache_fn: t.Callable[[TFlags, str], TFlags] = partial(
        populate_flag_cache_from_local,
        component_paths=[
            Path("sources"),
            Path("transforms"),
            Path("publishers"),
            Path.home(),
        ],
    ),
) -> TFlags:
    """Get flags for a specific component id populating the cache if needed.

    Args:
        component_id: The component to find. In the form of <source|transform|publisher>:<name>
        populate_cache_fn: A function which takes a cache dict and component id and populates
            the cache. The implementer can make decisions on how to interface with the network
            given the supplied component id which is a prefix for the requested flags.

    Returns:
        dict: The subset of flags related to the component, empty dict if no flags found
    """
    if not FLAGS or not any(k.startswith(component_id) for k in FLAGS):
        populate_cache_fn(FLAGS, component_id)
    subset = {k: v for k, v in FLAGS.items() if k.startswith(component_id)}
    return subset
