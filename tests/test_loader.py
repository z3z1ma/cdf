import pytest

import cdf.core.registry as registry
from cdf.core.exception import SourceDirectoryEmpty, SourceDirectoryNotFoundError
from cdf.core.loader import DirectoryLoader


def test_load_sources():
    # Test case 1: Attempt to load sources from a non-existent directory
    with pytest.raises(SourceDirectoryNotFoundError):
        DirectoryLoader("./tests/fixtures/non-existent")

    # Test case 2: Attempt to load sources from a non-directory
    with pytest.raises(SourceDirectoryNotFoundError):
        DirectoryLoader("./tests/fixtures/source1.py")

    # Test case 3: Attempt to load sources from an empty directory
    with pytest.raises(SourceDirectoryEmpty):
        DirectoryLoader("./tests/fixtures/empty")

    # Test case 4: Load sources from a valid directory, ensure source is registered
    loader = DirectoryLoader("./tests/fixtures/sources")
    assert len(loader.cache) == 1
    assert loader.executions == 1
    _ = loader.cache["source1"]()
    assert registry.has_source("source1")
