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
        "source:source1:gen": True,
        "source:mocksource:someresource": True,
        "source:pokemon:berries": False,
        "source:pokemon:pokemon": True,
        "source:chess_player_data:players_archives": False,
        "source:chess_player_data:players_games": True,
        "source:chess_player_data:players_online_status": True,
        "source:chess_player_data:players_profiles": True,
    }

    # Test case 2: Can get flags for a component with a parameterized
    # populate_cache_fn, in this case we use the local implementation
    # but we could use a harness.io implementation
    mocker.patch(
        "cdf.core.feature_flags.FLAGS", {"source:source1:cachedresource": False}
    )
    cache_fn = partial(
        populate_flag_cache_from_local,
        component_paths=[Path("tests/fixtures/basic_sources")],
    )
    flags = get_flags_for_component("source:mocksource", cache_fn)
    assert flags == {"source:mocksource:someresource": True}
    flags = get_flags_for_component("source:source1", cache_fn)
    assert flags == {
        "source:source1:gen": True,
        "source:source1:cachedresource": False,
    }
