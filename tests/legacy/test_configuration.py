# pyright: reportAttributeAccessIssue=false, reportPrivateUsage=false
"""Tests for the context module."""

import datetime
import os
import tempfile
from pathlib import Path

import pytest

from cdf.legacy.configuration import (
    _CONVERTERS,
    ConfigBox,
    ConfigurationLoader,
    add_custom_converter,
    get_converter,
    remove_custom_converter,
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


def test_custom_converter_integration():
    """Test custom converter functionality by adding a custom 'double' converter."""

    def multiply_by_two(value: str) -> int:
        return int(value) * 2

    add_custom_converter("double", multiply_by_two)
    loader = ConfigurationLoader({"value": "@double 21"}, include_envvars=False)
    config = loader.load()

    assert config.value == 42
    _CONVERTERS.pop("double")


def test_add_custom_converter():
    """Test adding a new custom converter."""

    def custom_converter(value: str) -> str:
        return f"custom_{value}"

    add_custom_converter("custom", custom_converter)
    assert get_converter("custom")("value") == "custom_value"
    remove_custom_converter("custom")  # Cleanup after test


def test_add_existing_custom_converter():
    """Test error when adding an already existing converter."""
    with pytest.raises(ValueError):
        add_custom_converter("int", lambda x: x)


def test_get_nonexistent_converter():
    """Test retrieving a non-existent converter."""
    with pytest.raises(KeyError):
        get_converter("nonexistent")


def test_remove_converter():
    """Test removing a converter successfully."""
    add_custom_converter("temp", lambda x: x)
    remove_custom_converter("temp")
    with pytest.raises(KeyError):
        get_converter("temp")


def test_loader_with_multiple_sources():
    """Test loading from multiple configuration sources."""
    loader = ConfigurationLoader({"key1": "value1"}, {"key2": "value2"})
    config = loader.load()
    assert config.key1 == "value1"
    assert config.key2 == "value2"


def test_loader_from_name_with_extensions():
    """Test loading configuration from a name by searching with extensions."""
    with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as tmp:
        tmp.write(b'{"name": "test"}')
        tmp_path = Path(tmp.name).with_suffix("")

    loader = ConfigurationLoader.from_name(tmp_path.stem, search_paths=[tmp_path.parent])
    config = loader.load()
    assert config.name == "test"
    os.remove(tmp.name)


def test_loader_resolution_strategy_scope():
    """Test scope-based resolution strategy in the configuration loader."""
    loader = ConfigurationLoader(
        {"key": "global_value"}, {"key": "scoped_value"}, resolution_strategy="scope"
    )
    config = loader.load()
    assert config.key == "scoped_value"


def test_loader_invalid_source_type():
    """Test loading from an invalid source type raises TypeError."""
    loader = ConfigurationLoader(123)  # type: ignore
    with pytest.raises(TypeError):
        loader.load()


def test_converter_box_apply_converters():
    """Test applying converters in the ConfigBox."""
    box = ConfigBox({"key": "@int 42"}, box_dots=True)
    assert box["key"] == 42


def test_converter_box_values():
    """Test accessing all values in ConfigBox with applied converters."""
    box = ConfigBox({"key1": "@int 42", "key2": "@bool True"}, box_dots=True)
    values = list(box.values())
    assert values == [42, True]


def test_converter_box_invalid_converter():
    """Test invalid converter raises ValueError."""
    box = ConfigBox({"key": "@unknown value"}, box_dots=True)
    with pytest.raises(ValueError):
        box["key"]


def test_converter_box_non_string_data():
    """Test that non-string data is a no-op in _apply_converters."""
    box = ConfigBox({"key": 42}, box_dots=True)
    assert box["key"] == 42


@pytest.mark.parametrize(
    "config_source, expected_result",
    [
        ({"list_value": "@list [1,2,3]"}, [1, 2, 3]),
        ({"bool_value": "@bool True"}, True),
        ({"dict_value": "@dict {'a':1,'b':2}"}, {"a": 1, "b": 2}),
        ({"datetime_value": "@datetime 2025-01-01 12:00"}, datetime.datetime(2025, 1, 1, 12)),
        ({"date_value_1": "@date 2025-01-01"}, datetime.date(2025, 1, 1)),
        ({"date_value_2": "@date 01/01/2025"}, datetime.date(2025, 1, 1)),
    ],
)
def test_various_converters(config_source, expected_result):
    """Test various converters."""
    loader = ConfigurationLoader(config_source, include_envvars=False)
    config = loader.load()
    assert list(config.values())[0] == expected_result


def test_converter_box_resolve_unknown_key():
    """Test resolve converter with an unknown key raises ValueError."""
    box = ConfigBox({"key": "@resolve unknown_key"}, box_dots=True)
    with pytest.raises(ValueError):
        box["key"]


def test_converter_box_empty_string():
    """Test empty string with converter pattern returns None."""
    box = ConfigBox({"key": "@int "}, box_dots=True)
    assert box["key"] is None


def test_configuration_loader_with_env(mock_environment):
    """Test that configuration values are loaded correctly with environment variable expansion."""
    config_sources = [
        {"name": "${USER}", "age": "@int 30"},
        {"model": "SVM", "num_iter": "@int 35"},
        lambda: {"processor": "add_one", "seq": "@tuple (1,2,3)"},
        lambda: {"model_A": "@float @resolve age"},
        {"dependency_paths": ["path/ok"]},
    ]
    loader = ConfigurationLoader(*config_sources, include_envvars=False)
    config = loader.load()

    assert config.name == "test_user"
    assert config.age == 30
    assert config.model == "SVM"
    assert config.num_iter == 35
    assert config.processor == "add_one"
    assert config.seq == (1, 2, 3)
    assert config.model_A == 30.0
    assert config.dependency_paths == ["path/ok"]
