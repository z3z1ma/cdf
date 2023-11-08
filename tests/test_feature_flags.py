import typing as t
from functools import partial
from pathlib import Path

import pytest
from dlt.common.configuration.container import Container
from dlt.common.configuration.specs.config_providers_context import (
    ConfigProvidersContext,
)

from cdf.core.config import find_cdf_config_providers
from cdf.core.feature_flags import get_component_ff, get_or_create_flag_dispatch


@pytest.fixture
def cdf_provider() -> t.Iterator[ConfigProvidersContext]:
    ctx = ConfigProvidersContext()
    ctx.providers.clear()
    ctx.add_provider(next(find_cdf_config_providers([Path("tests/fixtures")])))
    with Container().injectable_context(ctx):
        yield ctx


def test_local_flags(cdf_provider):
    _ = cdf_provider
    # Test case 1: Can populate cache from local files
    # Cache is merged from multiple files based on traversing the directory tree
    # Furthermore the passed cache is mutated in place
    cache = {}
    get_or_create_flag_dispatch(
        cache, "source:source1:gen", component_paths=[Path("tests/fixtures")]
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
    inline_cache = {"source:source1:cachedresource": False}
    cache_fn = partial(
        get_or_create_flag_dispatch,
        component_paths=[Path("tests/fixtures/basic_sources")],
    )
    flags = get_component_ff("source:mocksource:someresource", cache_fn, inline_cache)
    assert flags == {"source:mocksource:someresource": True}
    assert inline_cache == {
        "source:mocksource:someresource": True,
        "source:source1:gen": True,
        "source:source1:cachedresource": False,
    }
