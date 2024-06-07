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
    """Ensure the project can be indexed.

    The project is a dictionary-like object. It exposes its own configuration,
    and it exposes workspaces through the `workspaces` key. Dot notation is also
    supported. Dunder methods like `__contains__` and `__len__` apply to
    the workspace collection. The project is read-only. Indexing into a Workspace
    object will invoke the workspace's __getitem__ method which also supports dot
    notation. Hence the project is a tree-like structure.
    """
    assert project["name"] == "cdf-example"
    assert project["version"] == "0.1.0"
    assert project["feature_flags.provider"] == "filesystem"
    assert project["workspaces.alex"] is project.get_workspace("alex").unwrap()
    assert len(project) == 1
    assert len(project["workspaces.alex.pipelines"]) == 3
    assert "alex" in project
    assert "jane" not in project
    with pytest.raises(KeyError):
        project["workspaces.jane"]
    with pytest.raises(NotImplementedError):
        del project["name"]
    assert list(project)[0] is project["workspaces.alex"]
    assert project["workspaces.alex.pipelines.us_cities.version"] == 1


def test_project_get_spec(project: Project):
    """Ensure the project can get a spec and that we get the same spec each time."""
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
