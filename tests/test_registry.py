import pytest

import cdf.core.registry as registry
from cdf.core.source import source


@pytest.fixture
def basic_source():
    return source(lambda: [], name="basic_source")


def test_registry_add(basic_source, mocker):
    mocker.patch("cdf.core.registry._sources", {})

    # Test case 1: Attempt to register None as a source
    with pytest.raises(TypeError):
        registry.register_source(None)  # type: ignore

    # Test case 2: Attempt to register a non-string value as a source
    with pytest.raises(TypeError):
        registry.register_source(123)  # type: ignore

    # Test case 3: Attempt to register a valid source
    registry.register_source(basic_source())

    # Assert that the source has been added to the registry
    assert len(registry._sources) == 1
    assert registry.has_source("basic_source")

    # Test case 4: Attempt to register a source via side effect
    @source(name="inline_source")
    def _xyz():
        def _abc():
            yield from range(10)

        return [_abc()]

    _ = _xyz()  # Setup
    assert len(registry._sources) == 2
    assert registry.has_source("inline_source")


def test_registry_remove(basic_source, mocker):
    mocker.patch("cdf.core.registry._sources", {})
    # Test case 1: Attempt to remove a source that does not exist
    with pytest.raises(AttributeError):
        registry.remove_source("basic_source")
    # Test case 2: Attempt to remove a source that does exist
    registry.register_source(basic_source())
    registry.remove_source("basic_source")
    # Assert that the source has been removed from the registry
    assert len(registry._sources) == 0


def test_registry_behavior(basic_source, mocker):
    mocker.patch("cdf.core.registry._sources", {})
    # Test case 1: Test __getattr__
    with pytest.raises(AttributeError):
        registry.basic_source

    # Test case 2: Test __getattr__ after adding a source
    registry.register_source(basic_source())
    registry.basic_source
