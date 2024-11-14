"""Tests for the context module."""

import asyncio
import atexit
import os
import pytest

from contextlib import AbstractContextManager
from cdf.core.container import (
    Container,
    DependencyCycleError,
    DependencyNotFoundError,
    active_container,
)
from cdf.core.configuration import ConfigurationLoader


class SampleResource(AbstractContextManager):
    def __init__(self):
        self.cleaned_up = False

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.cleaned_up = True


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
    return ConfigurationLoader("tests/fixtures/conf.yaml", include_envvars=False)


@pytest.fixture
def basic_context(simple_loader: ConfigurationLoader):
    """Fixture to create a context with the simple loader."""
    return Container(simple_loader.load())


@pytest.fixture
def context():
    return Container()


def test_basic_dependency_injection(basic_context: Container):
    """Test basic dependency injection functionality within the context."""
    basic_context.config = {"db_url": "sqlite:///:memory:"}

    @basic_context.register_dep("db_connection")
    def _(C):
        return f"Connected to {C.config.db_url}"

    @basic_context.register_dep("repo")
    def _(db_connection):
        return f"Repo using {db_connection}"

    @basic_context
    def business_logic(repo):
        return f"Logic with {repo}"

    result = business_logic()
    assert result == "Logic with Repo using Connected to sqlite:///:memory:"


def test_add_and_retrieve_singleton_dependency(basic_context: Container):
    """Test adding a singleton dependency using a factory."""
    count = 0

    def singleton_dep():
        nonlocal count
        count += 1
        return f"instance_{count}"

    basic_context.add_factory("singleton_dep", singleton_dep, singleton=True)
    instance_1 = basic_context.get("singleton_dep")
    instance_2 = basic_context.get("singleton_dep")
    assert instance_1 == instance_2
    assert count == 1


def test_add_and_retrieve_transient_dependency(basic_context):
    """Test adding a transient dependency using a factory."""
    count = 0

    def transient_dep():
        nonlocal count
        count += 1
        return f"instance_{count}"

    basic_context.add_factory("transient_dep", transient_dep, singleton=False)
    instance_1 = basic_context.get("transient_dep")
    instance_2 = basic_context.get("transient_dep")
    assert instance_1 != instance_2
    assert count == 2


def test_register_dependency_with_decorator(basic_context: Container):
    """Test registering a dependency using the register_dep decorator."""

    @basic_context.register_dep("decorated_dep")
    def _():
        return "Decorated Dependency"

    assert basic_context.get("decorated_dep") == "Decorated Dependency"


def test_inject_deps_decorator(basic_context: Container):
    """Test injecting dependencies into a function with inject_deps."""
    basic_context.add("dep", "injected_value")

    @basic_context.inject_deps
    def func(dep):
        return dep

    assert func() == "injected_value"


def test_dependency_cycle_detection(basic_context: Container):
    """Test detection of cyclic dependencies."""

    @basic_context.register_dep("service_a")
    def _(service_b):
        return "Service A" + service_b

    @basic_context.register_dep("service_b")
    def _(service_a):
        return "Service B" + service_a

    with pytest.raises(DependencyCycleError):
        basic_context.get("service_a")


def test_add_and_inject_multiple_dependencies(basic_context: Container):
    """Test adding and injecting multiple dependencies into a function."""
    basic_context.add("db_conn", "db_connection")
    basic_context.add("cache", "cache_service")

    @basic_context.inject_deps
    def process(db_conn, cache):
        return f"{db_conn} and {cache}"

    assert process() == "db_connection and cache_service"


def test_namespaced_contexts():
    """Test dependency injection with namespaced contexts to prevent collisions."""
    parent = Container(namespace="parent")
    child = Container(namespace="child", parent=parent)

    @parent.register_dep("service")
    def _():
        return "Service in parent"

    @child.register_dep("service")
    def _():
        return "Service in child"

    assert child.get("service") == "Service in child"
    assert parent.get("service") == "Service in parent"


def test_context_management(basic_context: Container):
    """Test active context management with context enter and exit."""
    with basic_context:
        assert active_container.get() is basic_context

    with pytest.raises(LookupError):
        active_container.get()  # No active context outside `with` block


def test_dependency_removal(basic_context: Container):
    """Test removing a dependency from the context."""

    @basic_context.register_dep("temp_service")
    def _():
        return "Temporary Service"

    assert basic_context.get("temp_service") == "Temporary Service"
    del basic_context["temp_service"]
    with pytest.raises(DependencyNotFoundError):
        basic_context.get("temp_service")


def test_async_dependencies(basic_context: Container):
    """Test asynchronous dependency injection."""

    @basic_context.register_dep
    async def async_service():
        await asyncio.sleep(0.1)
        return "Async Service"

    result = asyncio.run(async_service())
    assert result == "Async Service"


def test_async_injection(basic_context: Container):
    """Test asynchronous function injection."""

    @basic_context.register_dep("async_dependency")
    async def _():
        await asyncio.sleep(0.1)
        return "Async Dependency"

    @basic_context
    async def async_function(async_dependency):
        return f"Received {async_dependency}"

    result = asyncio.run(async_function())
    assert result == "Received Async Dependency"


def test_dependency_with_namespace():
    """Test namespaced dependency registration and retrieval."""
    context = Container(namespace="main")

    @context.register_dep("db_service", namespace="db")
    def _():
        return "DB Service"

    assert context.get("db_service", namespace="db") == "DB Service"
    with pytest.raises(DependencyNotFoundError):
        context.get("db_service")  # Default namespace has no "db_service"


def test_combined_contexts_with_conflicts():
    """Test combining contexts with conflicting dependency names across namespaces."""
    context1 = Container({"name": "Context1"}, namespace="ns1")
    context2 = Container({"age": 42}, namespace="ns2")

    @context1.register_dep("service")
    def _():
        return "Service from ns1"

    @context2.register_dep("service")
    def _():
        return "Service from ns2"

    combined_context = context1.combine(context2)
    assert combined_context.get("service", namespace="ns1") == "Service from ns1"
    assert combined_context.get("service", namespace="ns2") == "Service from ns2"


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
    loader = ConfigurationLoader(config_source, include_envvars=False)
    config = loader.load()
    assert list(config.values())[0] == expected_result


def test_dependency_not_found_error(basic_context: Container):
    """Test that accessing a non-existent dependency raises DependencyNotFoundError."""
    with pytest.raises(DependencyNotFoundError):
        basic_context.get("nonexistent")


@pytest.mark.parametrize(
    "param, expected",
    [
        ("localhost", "Connected to DB at localhost"),
        ("remote", "Connected to DB at remote"),
    ],
)
def test_dependency_with_parameters(
    basic_context: Container, param: str, expected: str
):
    """Test parameterized dependencies with context configuration."""

    @basic_context.register_dep("db_connection")
    def _(C):
        return f"Connected to DB at {C.config.db_host}"

    basic_context.config.db_host = param
    assert basic_context.get("db_connection") == expected


def test_dependency_with_default_value(basic_context: Container):
    """Test retrieving a dependency with a default value."""
    assert basic_context.get("optional_dep", default="default_value") == "default_value"


def test_resource_cleanup_on_exit(context: Container):
    context.add_factory("sample_resource", SampleResource, singleton=False)
    with context as ctx:
        retrieved_resource = ctx.get("sample_resource")
        assert not retrieved_resource.cleaned_up
    assert retrieved_resource.cleaned_up

    @context.wire
    def foo(sample_resource):
        assert not sample_resource.cleaned_up
        return sample_resource

    func_resource = foo()
    assert func_resource.cleaned_up

    @context.wire
    def bar():
        @context.wire
        def baz(sample_resource):
            assert not sample_resource.cleaned_up
            return sample_resource

        # ensure the resource is only cleaned up after the context.wire stack unwinds
        scoped_resource = baz()
        assert not scoped_resource.cleaned_up
        return scoped_resource

    nested_resource = bar()
    assert nested_resource.cleaned_up

    # ensure singletons are cleaned up on exit
    context.add_factory("singleton_resource", SampleResource, singleton=True)
    s_resource = context.get("singleton_resource")
    assert not s_resource.cleaned_up
    atexit._run_exitfuncs()
    assert s_resource.cleaned_up
