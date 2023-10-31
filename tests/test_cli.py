from typer.testing import CliRunner

from cdf.cli import app

runner = CliRunner()


def test_help():
    result = runner.invoke(app, ["--help"])
    assert result.exit_code == 0


def test_index():
    result = runner.invoke(app, ["-p", "./tests/fixtures/basic_sources", "index"])
    assert result.exit_code == 0
    assert "source1" in result.stdout

    result = runner.invoke(app, ["-p", "./tests/fixtures/empty", "index"])
    assert result.exit_code == 1

    # Uses partials
    result = runner.invoke(app, ["-p", "./tests/fixtures/sources", "index"])
    assert result.exit_code == 0
    assert "pokemon" in result.stdout
