"""Tests for the core.configuration module."""

import pytest

from cdf.core.configuration import load_config


def test_load_config():
    """Test the load_config function."""
    project = load_config("examples/sandbox")
    assert project.is_ok()

    with pytest.raises(FileNotFoundError):
        load_config("examples/idontexist").unwrap()

    # Unwrap the configuration
    project = project.unwrap()

    # Project config can be indexed directly, this gets the project name
    assert project["name"] == "cdf-test"

    # Namespaced access by indexing with a tuple (workspace, key)
    assert project[("workspace1", "name")] == "workspace1"

    # Namespaced access falls back to the project settings
    # in this way, workspace settings can override project settings if needed
    # This is the expected access pattern
    assert project[("workspace1", "filesystem.provider")] == "local"

    # Access by attribute works too, but does not support the
    # behavior of falling back to the project settings since we are directly accessing
    # the workspace settings
    assert (
        project[("workspace1", "pipelines.us_cities")]
        == project.workspace1.pipelines.us_cities
    )
