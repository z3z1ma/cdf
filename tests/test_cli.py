from typer.testing import CliRunner

from cdf.cli import app

runner = CliRunner()


def test_help():
    # Basic sanity check
    result = runner.invoke(app, ["--help"])
    assert result.exit_code == 0


def test_index(mocker):
    # Protect mut state
    mocker.patch("cdf.cli.CACHE", {})

    # Ensure invoking at root of workspace works
    result = runner.invoke(app, ["-p", "./tests/fixtures", "index"])
    assert result.exit_code == 0
    assert "source1" in result.stdout

    # Ensure we traverse upwards and find the workspace in empty dir
    result = runner.invoke(app, ["-p", "./tests/fixtures/empty", "index"])
    assert result.exit_code == 0

    # Ensure we traverse upwards and find the workspace in existing single project dir
    result = runner.invoke(app, ["-p", "./tests/fixtures/ut_project", "index"])
    assert result.exit_code == 0
    assert "pokemon" in result.stdout
