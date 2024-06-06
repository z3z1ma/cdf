"""Tests for the core.configuration module."""

import pytest

from cdf.core.project import load_project


def test_load_config():
    """Test the load_config function."""
    project = load_project("examples/sandbox")
    assert project.is_ok()

    with pytest.raises(FileNotFoundError):
        load_project("examples/idontexist").unwrap()

    # Unwrap the configuration
    project = project.unwrap()

    # Project config can be indexed directly, this gets the project name
    assert project["name"] == "cdf-example"

    spec = (
        project.get_workspace("alex")
        .bind(lambda workspace: workspace.get_pipeline_spec("us_cities"))
        .unwrap()
    )
    assert spec["name"] == "us_cities"

    import dlt

    with project.inject_context():
        assert dlt.config["something"] == "ok"
        dlt.config["other"] = "cool"
        assert dlt.config["other"] == "cool"
        dlt.secrets["ok.nice.cool"] = "wow"
        assert dlt.secrets["ok.nice.cool"] == "wow"
