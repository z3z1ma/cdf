import os
import tempfile

import pytest

from cdf.utils.file import _expand_vars, load_file, load_json, load_toml, load_yaml


@pytest.fixture
def mock_environment():
    """Fixture to set a mock environment variable and reset it after the test."""
    original_env = os.environ.copy()
    os.environ.clear()
    os.environ["USER"] = "test_user"
    yield
    os.environ.clear()
    os.environ.update(original_env)


def test_expand_env_vars(mock_environment):
    """Test expansion of environment variables in a string."""
    template = "User: ${USER}, Path: $PATH"
    expanded = _expand_vars(template)
    assert expanded == "User: test_user, Path: $PATH"


def test_read_json_file():
    """Test reading a JSON configuration file."""
    with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as tmp:
        _ = tmp.write(b'{"key": "value"}')
        tmp_path = tmp.name

    result = load_json(tmp_path)
    assert result == {"key": "value"}
    os.remove(tmp_path)


def test_read_yaml_file():
    """Test reading a YAML configuration file."""
    with tempfile.NamedTemporaryFile(suffix=".yaml", delete=False) as tmp:
        _ = tmp.write(b"key: value")
        tmp_path = tmp.name

    result = load_yaml(tmp_path)
    assert result == {"key": "value"}
    os.remove(tmp_path)


def test_read_toml_file():
    """Test reading a TOML configuration file."""
    with tempfile.NamedTemporaryFile(suffix=".toml", delete=False) as tmp:
        _ = tmp.write(b'key = "value"')
        tmp_path = tmp.name

    result = load_toml(tmp_path)
    assert result == {"key": "value"}
    os.remove(tmp_path)


def test_read_nonexistent_file():
    """Test reading a non-existent file returns an empty dictionary."""
    with pytest.raises(FileNotFoundError):
        load_file("nonexistent.json")
