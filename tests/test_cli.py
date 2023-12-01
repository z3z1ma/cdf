from typer.testing import CliRunner

from cdf.cli import app

runner = CliRunner()


def test_help():
    # Basic sanity check
    result = runner.invoke(app, ["--help"])
    assert result.exit_code == 0


def test_index():
    # Ensure invoking at root of workspace works
    result = runner.invoke(app, ["-p", "./examples/multi_workspace", "index"])
    assert result.exit_code == 0
    assert "dota2" in result.stdout

    # Ensure we traverse upwards and find the workspace
    result = runner.invoke(
        app, ["-p", "./examples/multi_workspace/other_stuff", "index"]
    )
    assert result.exit_code == 0
