from functools import partial
from pathlib import Path

from cdf.core.feature_flags import (
    get_flags_for_component,
    populate_flag_cache_from_local,
)


def test_local_flags(mocker):
    # Test case 1: Can populate cache from local files
    # Cache is merged from multiple files based on traversing the directory tree
    # Furthermore the passed cache is mutated in place
    cache = {}
    populate_flag_cache_from_local(
        cache, component_paths=[Path("tests/fixtures/sources")]
    )
    assert cache == {
        "source:source1:test_flag": True,
        "source:test_component:test_flag": True,
        "source:pokemon:berries:enabled": True,
        "source:pokemon:pokemon:enabled": True,
    }

    # Test case 2: Can get flags for a component with a parameterized
    # populate_cache_fn, in this case we use the local implementation
    # but we could use a harness.io implementation
    mocker.patch(
        "cdf.core.feature_flags.FLAGS", {"source:source1:existing_cached_flag": False}
    )
    cache_fn = partial(
        populate_flag_cache_from_local,
        component_paths=[Path("tests/fixtures/basic_sources")],
    )
    flags = get_flags_for_component("source:test_component", cache_fn)
    assert flags == {"source:test_component:test_flag": True}
    flags = get_flags_for_component("source:source1", cache_fn)
    assert flags == {
        "source:source1:test_flag": True,
        "source:source1:existing_cached_flag": False,
    }
