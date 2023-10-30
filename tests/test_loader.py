import pytest

import cdf.core.registry as registry
from cdf.core.exception import SourceDirectoryEmpty, SourceDirectoryNotFoundError
from cdf.core.loader import get_directory_modules, load_sources


def test_load_sources():
    # Test case 1: Attempt to load sources from a non-existent directory
    with pytest.raises(SourceDirectoryNotFoundError):
        load_sources(
            get_modules_fn=lambda: get_directory_modules(
                "./tests/fixtures/non-existent"
            )
        )

    # Test case 2: Attempt to load sources from a non-directory
    with pytest.raises(SourceDirectoryNotFoundError):
        load_sources(
            get_modules_fn=lambda: get_directory_modules("./tests/fixtures/source1.py")
        )

    # Test case 3: Attempt to load sources from an empty directory
    with pytest.raises(SourceDirectoryEmpty):
        load_sources(
            get_modules_fn=lambda: get_directory_modules("./tests/fixtures/empty")
        )

    # Test case 4: Load sources from a valid directory, ensure source is registered
    load_sources(
        get_modules_fn=lambda: get_directory_modules("./tests/fixtures/sources"),
        lazy_sources=False,
    )

    # assert len(loader.cache) == 1
    # assert loader.executions == 1
    # _ = loader.cache["source1"]()
    assert registry.has_source("source1")
