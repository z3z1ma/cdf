import os
import typing as t
from pathlib import Path

import dlt
import pytest
from dlt.common.configuration.container import Container
from dlt.common.configuration.specs.config_providers_context import (
    ConfigProvidersContext,
)

from cdf.core.config import find_cdf_config_providers
from cdf.core.feature_flags import (
    _component_id_to_harness_id,
    _harness_id_to_component_id,
    get_or_create_flag_dispatch,
    get_source_flags,
)


@pytest.fixture
def cdf_provider() -> t.Iterator[ConfigProvidersContext]:
    """Config provider which gives a test function an isolated context with cdf_config.toml loaded"""
    ctx = ConfigProvidersContext()
    ctx.add_provider(next(find_cdf_config_providers([Path("tests/fixtures")])))
    with Container().injectable_context(ctx):
        yield ctx


@pytest.fixture
def mocksource():
    """A mock source that shadows the source in the fixtures directory"""
    f = dlt.resource(
        iter(
            [
                {"a": 1, "b": True, "c": "foo"},
                {"a": 2, "b": False, "c": "bar"},
                {"a": 3, "b": True, "c": "baz"},
            ]
        ),
        name="someresource",
    )
    return dlt.source(lambda: f, name="mocksource")


def test_harness_id_to_component_id():
    assert (
        _component_id_to_harness_id("source:awesome.pokemon:pokemon")
        == "awesome_X_pokemon__pokemon"
    )
    assert (
        _harness_id_to_component_id("awesome_X_pokemon__pokemon")
        == "source:awesome.pokemon:pokemon"
    )


def test_local_flags(cdf_provider, mocksource, mocker):
    _ = cdf_provider
    # patch write_text in Path
    mocker.patch("pathlib.Path.write_text", return_value=10)

    # Test case 1: Can populate a flag cache from local files
    # Note: Local cache provider grabs all flags regardless of passed source
    # It is up to the provider to filter flags using the source if they want.
    cache = {}
    get_or_create_flag_dispatch(
        cache,
        mocksource(),
        workspace_name="ci",
        workspace_path=Path.cwd(),
        component_paths=[Path("tests/fixtures")],
    )
    assert cache == {
        "source:ci.source1:gen": True,
        "source:ci.mocksource:someresource": True,
    }

    # Test case 2: Cache is merged from multiple files based on traversing the
    # directory tree. (No workspace)
    cache = {}
    get_or_create_flag_dispatch(
        cache,
        mocksource(),
        workspace_name="awesome",
        workspace_path=Path.cwd(),
        component_paths=[Path("tests/fixtures/it_project/sources")],
    )
    assert cache == {
        "source:awesome.source1:gen": True,
        "source:awesome.mocksource:someresource": True,
        "source:awesome.pokemon:berries": False,
        "source:awesome.pokemon:pokemon": True,
        "source:awesome.chess_player_data:players_archives": False,
        "source:awesome.chess_player_data:players_games": True,
        "source:awesome.chess_player_data:players_online_status": True,
        "source:awesome.chess_player_data:players_profiles": True,
    }

    # Test case 3: Cached flags are merged with new flags
    cache = {"source:otherworkspace.users": True}
    get_or_create_flag_dispatch(
        cache,
        mocksource(),
        workspace_name="ci",
        workspace_path=Path.cwd(),  # No workspace
        component_paths=[Path("tests/fixtures")],
    )
    assert cache == {
        "source:ci.source1:gen": True,
        "source:ci.mocksource:someresource": True,
        "source:otherworkspace.users": True,
    }

    # Test case 4: Flags are gathered from all workspaces in multi-workspace project
    # default component paths (most typical usage pattern)
    # This will also not traverse higher than the workspace path
    cache = {}
    get_or_create_flag_dispatch(
        cache,
        mocksource(),
        workspace_name="awesome",
        workspace_path=Path("tests/fixtures/it_project"),
    )
    assert cache == {
        "source:awesome.pokemon:berries": False,
        "source:awesome.pokemon:pokemon": True,
        "source:awesome.chess_player_data:players_archives": False,
        "source:awesome.chess_player_data:players_games": True,
        "source:awesome.chess_player_data:players_online_status": True,
        "source:awesome.chess_player_data:players_profiles": True,
    }

    # Test case 5: Our primary entrypoint, get flags relevant to a specific source
    # The populate_cache_fn makes the function composable as the only requirement is
    # to take a set of inputs and mutate and return a cache dict, for this test we use
    # the default implementation which dispatches based on config / env vars.
    # The dict keys are source:<workspace>.<source_name>:<resource_name> as seen above
    src = mocksource()
    cache = {}
    get_source_flags(
        src,
        cache=cache,
        ns="ci",
        path=Path("tests/fixtures"),
    )
    assert src in cache
    assert cache[src] == {
        "source:ci.source1:gen": True,
        "source:ci.mocksource:someresource": True,
    }


@pytest.mark.skipif("FF__HARNESS__SDK_KEY" not in os.environ)
def test_harness_flags(cdf_provider, mocksource):
    _ = cdf_provider

    # Test case 1: Can populate a flag cache from harness.io
    # Also ensure correct translation from harness id -> component id
    cache = {}
    get_or_create_flag_dispatch(
        cache,
        mocksource(),
        workspace_name="ci",
        workspace_path=Path.cwd(),
        provider="harness",
    )
    assert "source:ci.mocksource:someresource" in cache
