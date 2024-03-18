"""Tests for the core.configuration module."""

import tempfile
from pathlib import Path

from cdf.core.configuration import load_project_config

CONFIG = """
default:
  name: cdf-test
  members:
  - examples/sandbox
  feature_flags:
    provider: local
    options:
      path: /tmp/flags.json
  filesystem:
    provider: local
    options:
      path: /tmp
production:
  feature_flags:
    provider: remote
    options:
      url: https://flags.example.com
"""


def test_load_config():
    """Test the load_config function."""
    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmpdir:
        with open(Path(tmpdir) / "cdf.yml", "w") as tmp:
            tmp.write(CONFIG)
        result = load_project_config(tmpdir)
    assert result.is_ok()
