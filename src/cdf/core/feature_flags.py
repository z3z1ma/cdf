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

import cdf.core.constants as c
from cdf.core.utils import do, search_merge_json

T = t.TypeVar("T")

TFlag = t.Union[str, t.Dict[str, t.Any], bool]
TFlags = t.Dict[str, TFlag]

FLAGS: TFlags = {}


# TODO: need an env var parser for flag implementations which are not local
# consideration for piggybacking on dlt's config resolver which is what we
# did in the original cdf, just annoying to worry on where the .dlt folder is
# relative to the user working directory...


def populate_flag_cache_from_harness(
    account_id: str, project_id: str, org_id: str
) -> TFlags:
    ...


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
    component_paths = component_paths or []
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
