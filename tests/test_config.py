import typing as t

import dlt
import pytest
from dlt.common.configuration.container import Container
from dlt.common.configuration.specs.config_providers_context import (
    ConfigProvidersContext,
)

from cdf.core.config import extend_global_providers, get_config_providers


@pytest.fixture
def empty_provider() -> t.Iterator[ConfigProvidersContext]:
    ctx = ConfigProvidersContext()
    ctx.providers.clear()
    with Container().injectable_context(ctx):
        yield ctx


def test_get_config(empty_provider):
    _ = empty_provider

    # Test case 1: Can get config from local files
    providers = get_config_providers(
        search_paths=["tests/fixtures/basic_sources"],
        search_cwd=False,
    )
    assert len(providers) == 1
    assert providers[0].name == "cdf_config.toml"

    # Test case 2: Can't get config from global config providers as it is not extended
    with pytest.raises(KeyError):
        dlt.config["ff.provider"]  # type: ignore[import]

    # Test case 3: Can update global config providers
    extend_global_providers(providers)

    # Test case 4: Can get config from global config providers
    assert dlt.config["ff.provider"] == "local"  # type: ignore[import]