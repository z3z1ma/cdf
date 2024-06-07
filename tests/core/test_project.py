"""Tests for the core.project module."""

import dlt
import pytest

from cdf.core.project import Project, load_project


def test_load_project():
    """Test the load_project function."""
    project = load_project("examples/sandbox")
    assert project.is_ok()

    err_monad = load_project("examples/idontexist")
    with pytest.raises(FileNotFoundError):
        err_monad.unwrap()

    project = project.unwrap()
    assert isinstance(project, Project)


@pytest.fixture
def project():
    """Load the project for testing."""
    return load_project("examples/sandbox").unwrap()


def test_project_indexing(project: Project):
    """Ensure the project can be indexed."""
    assert project["name"] == "cdf-example"


def test_project_get_spec(project: Project):
    """Ensure the project can get a spec."""
    spec = (
        project.get_workspace("alex")
        .bind(lambda workspace: workspace.get_pipeline_spec("us_cities"))
        .unwrap()
    )
    assert spec["name"] == "us_cities"
    assert callable(spec)
    assert spec is (
        project.get_workspace("alex")
        .bind(lambda workspace: workspace.get_pipeline_spec("us_cities"))
        .unwrap()
    )


def test_inject_configuration(project: Project):
    """Ensure keys are persisted while injecting configuration."""
    with project.inject_configuration():
        assert dlt.config["something"] == "ok"
        dlt.config["other"] = "cool"
        assert dlt.config["other"] == "cool"
        dlt.secrets["ok.nice.cool"] = "wow"
        assert dlt.secrets["ok.nice.cool"] == "wow"


def test_round_trip_serialization(project: Project):
    """Test that the project can be serialized and deserialized."""
    obj = project.model_dump()
    roundtrip = Project.model_validate(obj)
    assert roundtrip == project
    assert roundtrip.is_newer_than(project)
