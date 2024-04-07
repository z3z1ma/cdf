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
    assert project["project"]["name"] == "cdf-example"
