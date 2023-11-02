import typing as t

import pytest
from dlt.common.configuration.container import Container
from dlt.common.configuration.specs.config_providers_context import (
    ConfigProvidersContext,
)
from typer.testing import CliRunner

from cdf.cli import app


@pytest.fixture
def empty_provider() -> t.Iterator[ConfigProvidersContext]:
    ctx = ConfigProvidersContext()
    ctx.providers.clear()
    with Container().injectable_context(ctx):
        yield ctx


runner = CliRunner()


def test_help():
    result = runner.invoke(app, ["--help"])
    assert result.exit_code == 0


def test_index(empty_provider, mocker):
    # Protect mut state
    _ = empty_provider
    mocker.patch("cdf.cli.CACHE", {})
    mocker.patch("cdf.core.constants.COMPONENT_PATHS", [])

    result = runner.invoke(app, ["-p", "./tests/fixtures/basic_sources", "index"])
    assert result.exit_code == 0
    assert "source1" in result.stdout

    result = runner.invoke(app, ["-p", "./tests/fixtures/empty", "index"])
    assert result.exit_code == 1

    # Uses partials
    result = runner.invoke(app, ["-p", "./tests/fixtures/sources", "index"])
    assert result.exit_code == 0
    assert "pokemon" in result.stdout
