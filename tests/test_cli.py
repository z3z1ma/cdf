from typer.testing import CliRunner

from cdf.cli import app

runner = CliRunner()


def test_index():
    result = runner.invoke(app, ["-p", "examples/sandbox", "-w", "alex", "index"])
    assert result.exit_code == 0
    assert "Pipelines" in result.stdout
    assert "Sinks" in result.stdout
