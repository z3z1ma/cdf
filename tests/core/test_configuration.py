"""Tests for the context module."""

import os
import pytest
from cdf.core.configuration import (
    _CONVERTERS,
    SimpleConfigurationLoader,
    add_custom_converter,
)


@pytest.fixture
def mock_environment():
    """Fixture to set a mock environment variable and reset it after the test."""
    original_env = os.environ.copy()
    os.environ.clear()
    os.environ["USER"] = "test_user"
    yield
    os.environ.clear()
    os.environ.update(original_env)


def test_configuration_loader_with_env(mock_environment):
    """Test that configuration values are loaded correctly with environment variable expansion."""
    config_sources = [
        {"name": "${USER}", "age": "@int 30"},
        {"model": "SVM", "num_iter": "@int 35"},
        lambda: {"processor": "add_one", "seq": "@tuple (1,2,3)"},
        lambda: {"model_A": "@float @resolve age"},
        {"dependency_paths": ["path/ok"]},
    ]
    loader = SimpleConfigurationLoader(*config_sources, include_envvars=False)
    config = loader.load()

    assert config.name == "test_user"
    assert config.age == 30
    assert config.model == "SVM"
    assert config.num_iter == 35
    assert config.processor == "add_one"
    assert config.seq == (1, 2, 3)
    assert config.model_A == 30.0
    assert config.dependency_paths == ["path/ok"]


def test_custom_converter_integration():
    """Test custom converter functionality by adding a custom 'double' converter."""

    def multiply_by_two(value: str) -> int:
        return int(value) * 2

    add_custom_converter("double", multiply_by_two)
    loader = SimpleConfigurationLoader({"value": "@double 21"}, include_envvars=False)
    config = loader.load()

    assert config.value == 42
    _CONVERTERS.pop("double")  # Cleanup after test


@pytest.mark.parametrize(
    "config_source, expected_result",
    [
        ({"list_value": "@list [1,2,3]"}, [1, 2, 3]),
        ({"bool_value": "@bool True"}, True),
        ({"dict_value": "@dict {'a':1,'b':2}"}, {"a": 1, "b": 2}),
    ],
)
def test_converters(config_source: dict, expected_result: object):
    """Test various built-in converters for list, bool, and dict values."""
    loader = SimpleConfigurationLoader(config_source, include_envvars=False)
    config = loader.load()
    assert list(config.values())[0] == expected_result
