"""Tests for the context module."""

import asyncio
import os
import pytest
from cdf.core.context import (
    _CONVERTERS,
    Context,
    DependencyCycleError,
    DependencyNotFoundError,
    SimpleConfigurationLoader,
    active_context,
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


@pytest.fixture
def simple_loader():
    """Fixture to provide a simple configuration loader."""
    return SimpleConfigurationLoader("tests/fixtures/conf.yaml", include_env=False)


@pytest.fixture
def basic_context(simple_loader: SimpleConfigurationLoader):
    """Fixture to create a context with the simple loader."""
    return Context(simple_loader)


def test_configuration_loader_with_env(mock_environment):
    """Test that configuration values are loaded correctly with environment variable expansion."""
    config_sources = [
        {"name": "${USER}", "age": "@int 30"},
        {"model": "SVM", "num_iter": "@int 35"},
        lambda: {"processor": "add_one", "seq": "@tuple (1,2,3)"},
        lambda: {"model_A": "@float @resolve age"},
        {"dependency_paths": ["path/ok"]},
    ]
    loader = SimpleConfigurationLoader(*config_sources, include_env=False)
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
    loader = SimpleConfigurationLoader({"value": "@double 21"}, include_env=False)
    config = loader.load()

    assert config.value == 42
    _CONVERTERS.pop("double")  # Cleanup after test


def test_basic_dependency_injection(basic_context: Context):
    """Test basic dependency injection functionality within the context."""
    basic_context.config.db_url = "sqlite:///:memory:"

    @basic_context.register_dep
    def db_connection(C):
        return f"Connected to {C.config.db_url}"

    @basic_context.register_dep
    def repo(db_connection):
        return f"Repo using {db_connection}"

    @basic_context
    def business_logic(repo):
        return f"Logic with {repo}"

    result = business_logic()
    assert result == "Logic with Repo using Connected to sqlite:///:memory:"


def test_singleton_and_transient_dependencies(basic_context: Context):
    """Test singleton vs transient dependency behaviors."""
    counter = {"count": 0}

    @basic_context.register_dep(singleton=True)
    def singleton_service():
        counter["count"] += 1
        return f"Instance {counter['count']}"

    @basic_context.register_dep(singleton=False)
    def transient_service():
        counter["count"] += 1
        return f"Instance {counter['count']}"

    assert basic_context.get("singleton_service") == basic_context.get(
        "singleton_service"
    )
    assert basic_context.get("transient_service") != basic_context.get(
        "transient_service"
    )


def test_namespaced_contexts():
    """Test dependency injection with namespaced contexts to prevent collisions."""
    loader = SimpleConfigurationLoader(include_env=False)
    parent = Context(loader, namespace="parent")
    child = Context(loader, namespace="child", parent=parent)

    @parent.register_dep
    def service(): # type: ignore
        return "Service in parent"

    @child.register_dep
    def service(): # type: ignore
        return "Service in child"

    assert child.get("service") == "Service in child"
    assert parent.get("service") == "Service in parent"


def test_async_dependencies(basic_context: Context):
    """Test asynchronous dependency injection."""

    @basic_context.register_dep
    async def async_service():
        await asyncio.sleep(0.1)
        return "Async Service"

    result = asyncio.run(async_service())
    assert result == "Async Service"


def test_async_injection(basic_context: Context):
    """Test asynchronous function injection."""

    @basic_context.register_dep("async_dependency")
    async def async_register_dep():
        await asyncio.sleep(0.1)
        return "Async Dependency"

    @basic_context
    async def async_function(async_dependency):
        return f"Received {async_dependency}"

    result = asyncio.run(async_function())
    assert result == "Received Async Dependency"


def test_dependency_removal(basic_context: Context):
    """Test removing a dependency from the context."""

    @basic_context.register_dep
    def temp_service():
        return "Temporary Service"

    assert basic_context.get("temp_service") == "Temporary Service"
    del basic_context["temp_service"]
    with pytest.raises(DependencyNotFoundError):
        basic_context.get("temp_service")


def test_dependency_with_namespace():
    """Test namespaced dependency registration and retrieval."""
    loader = SimpleConfigurationLoader(include_env=False)
    context = Context(loader, namespace="main")

    @context.register_dep(namespace="db")
    def db_service():
        return "DB Service"

    assert context.get("db_service", namespace="db") == "DB Service"
    with pytest.raises(DependencyNotFoundError):
        context.get("db_service")  # Default namespace has no "db_service"


def test_combined_contexts_with_conflicts():
    """Test combining contexts with conflicting dependency names across namespaces."""
    loader1 = SimpleConfigurationLoader({"name": "Context1"}, include_env=False)
    context1 = Context(loader1, namespace="ns1")

    loader2 = SimpleConfigurationLoader({"age": 42}, include_env=False)
    context2 = Context(loader2, namespace="ns2")

    @context1.register_dep
    def service(): # type: ignore
        return "Service from ns1"

    @context2.register_dep
    def service(): # type: ignore
        return "Service from ns2"

    combined_context = context1.combine(context2)
    assert combined_context.get("service", namespace="ns1") == "Service from ns1"
    assert combined_context.get("service", namespace="ns2") == "Service from ns2"


def test_reload_config():
    """Test configuration reloading after adding a new source."""
    loader = SimpleConfigurationLoader({"value": 1}, include_env=False)
    context = Context(loader)
    assert context.config.value == 1

    loader.add_source({"value": 2})
    context.reload_config()
    assert context.config.value == 2


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
    loader = SimpleConfigurationLoader(config_source, include_env=False)
    config = loader.load()
    assert list(config.values())[0] == expected_result


def test_dependency_cycle_detection(basic_context: Context):
    """Test detection of cyclic dependencies."""

    @basic_context.register_dep
    def service_a(service_b):
        return "Service A"

    @basic_context.register_dep
    def service_b(service_a):
        return "Service B"

    with pytest.raises(DependencyCycleError):
        basic_context.get("service_a")


def test_dependency_not_found_error(basic_context: Context):
    """Test that accessing a non-existent dependency raises DependencyNotFoundError."""
    with pytest.raises(DependencyNotFoundError):
        basic_context.get("nonexistent")


def test_context_management(basic_context: Context):
    """Test active context management with context enter and exit."""
    with basic_context:
        assert active_context.get() is basic_context

    with pytest.raises(LookupError):
        active_context.get()  # No active context outside `with` block


@pytest.mark.parametrize(
    "param, expected",
    [
        ("localhost", "Connected to DB at localhost"),
        ("remote", "Connected to DB at remote"),
    ],
)
def test_dependency_with_parameters(basic_context: Context, param: str, expected: str):
    """Test parameterized dependencies with context configuration."""

    @basic_context.register_dep
    def db_connection(C):
        return f"Connected to DB at {C.config.db_host}"

    basic_context.config.db_host = param
    assert basic_context.get("db_connection") == expected
