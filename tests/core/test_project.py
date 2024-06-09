"""Tests for the core.project module."""

from pathlib import Path

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
    assert (
        project["workspaces.alex.scripts.nested/hello"]
        == roundtrip["workspaces.alex.scripts.nested/hello"]
    )


def test_init_ff(project: Project):
    """Test that the feature flag adapter is initialized."""
    assert project.ff_adapter is not None
    assert project.ff.provider == "filesystem"


def test_init_fs(project: Project):
    """Test that the filesystem adapter is initialized."""
    assert project.fs_adapter is not None
    assert project.fs_adapter.protocol == "cdf"


def test_init_state(project: Project):
    """Test that the state adapter is initialized."""
    from sqlglot import exp

    adapter = project.state.create_engine_adapter()
    assert adapter is not None
    adapter.create_schema("test")
    adapter.create_table("test1", {"name": exp.DataType.build("text")})
    assert adapter.table_exists("test1")
    adapter.close()


@pytest.fixture
def python_project():
    city_spec = {
        "path": Path("pipelines/us_cities_pipeline.py"),
        "cron_string": "@daily",
        "description": "Get US city data",
        "metrics": {
            "*": [
                {
                    "name": "cdf_builtin_metrics_count",
                    "description": "Counts the number of items in a dataset",
                    "entrypoint": "cdf.builtin.metrics:count",
                },
                {
                    "name": "cdf_builtin_metrics_max_value",
                    "description": "Returns the maximum value of a key in a dataset",
                    "entrypoint": "cdf.builtin.metrics:max_value",
                    "options": {"key": "zip_code"},
                },
            ]
        },
        "filters": {},
        "dataset_name": "test_city",
        "options": {
            "progress": None,
            "full_refresh": False,
            "loader_file_format": "insert_values",
            "runtime": {"dlthub_telemetry": False},
        },
    }
    dota_spec = {
        "cron_string": "@daily",
        "name": "dota2",
        "description": "Dota2 is a Massive Online Battle Arena game based on Warcraft.",
        "path": Path("pipelines/dota2_pipeline.py"),
    }
    local_spec = {
        "name": "local",
        "description": "No description provided.",
        "path": Path("sinks/local_sink.py"),
    }
    httpbin_spec = {
        "cron_string": "@daily",
        "name": "httpbin",
        "description": "A publisher that pushes data to httpbin.org",
        "path": Path("publishers/httpbin_publisher.py"),
        "depends_on": ["mart.zips"],
    }
    hello_spec = {
        "cron_string": "@daily",
        "name": "hello",
        "description": "No description provided.",
        "path": Path("scripts/hello_script.py"),
    }
    return Project.model_validate(
        {
            "path": Path("examples/sandbox").resolve(),
            "name": "data-platform",
            "version": "0.2.0",
            "workspaces": {
                "datateam": {
                    "path": "alex",
                    "pipelines": {"cities": city_spec, "dota": dota_spec},
                    "sinks": {"local": local_spec},
                    "publishers": {"httpbin": httpbin_spec},
                    "scripts": {"hello": hello_spec},
                }
            },
            "filesystem": {"uri": "file://_storage", "options": {}},
            "feature_flags": {
                "provider": "filesystem",
                "filename": "@jinja dev_flags_{{ 1 + 1}}.json",
            },
        }
    )


def test_custom_project(python_project: Project):
    """Test creating a project programmatically.

    This project has a custom structure and is not loaded from a file. Components
    are still ultimately based on python files, however the configuration wrapping
    these components is done in code which offers more flexibility.
    """
    assert python_project.name == "data-platform"


@pytest.fixture
def barebones_project():
    return Project.model_validate(
        {
            "path": "examples/sandbox",
            "name": "data-platform",
            "workspaces": {
                "datateam": {
                    "path": "alex",
                    "pipelines": {
                        "cities": "pipelines/us_cities_pipeline.py",
                        "dota": {"path": "pipelines/dota2_pipeline.py"},
                    },
                    "sinks": {"local": "sinks/local_sink.py"},
                    "publishers": {
                        "httpbin": {
                            "path": "publishers/httpbin_publisher.py",
                            "depends_on": ["mart.zips"],
                        }
                    },
                    "scripts": {"hello": "scripts/hello_script.py"},
                }
            },
        }
    )


def test_barebones_project(barebones_project: Project):
    """Test creating a project programmatically with minimal configuration.

    This asserts that certain heuristics are applied to the configuration to
    make it more user-friendly.
    """
    assert barebones_project.name == "data-platform"
    assert barebones_project["workspaces.datateam.pipelines.cities"] is not None
    assert barebones_project["workspaces.datateam.publishers.httpbin.depends_on"] == [
        "mart.zips"
    ]
    assert barebones_project["workspaces.datateam.sinks.local.component_path"] == Path(
        "sinks/local_sink.py"
    )
    assert barebones_project[
        "workspaces.datateam.scripts.hello.component_path"
    ] == Path("scripts/hello_script.py")
    assert barebones_project[
        "workspaces.datateam.pipelines.cities.component_path"
    ] == Path("pipelines/us_cities_pipeline.py")
    assert len(barebones_project["workspaces.datateam.pipelines"]) == 2
    assert len(barebones_project["workspaces.datateam.sinks"]) == 1
    assert len(barebones_project["workspaces.datateam.publishers"]) == 1
    assert len(barebones_project["workspaces.datateam.scripts"]) == 1
    assert len(barebones_project["workspaces.datateam"]) == 5
    assert len(barebones_project) == 1
    assert "datateam" in barebones_project
    assert "jane" not in barebones_project
    with pytest.raises(KeyError):
        barebones_project["workspaces.jane"]
    with pytest.raises(NotImplementedError):
        del barebones_project["name"]
    assert list(barebones_project)[0] is barebones_project["workspaces.datateam"]
