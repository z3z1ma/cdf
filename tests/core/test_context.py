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
def mock_env():
    original_env = os.environ.copy()
    os.environ["USER"] = "test_user"
    yield
    os.environ.clear()
    os.environ.update(original_env)


def test_configuration_loading(mock_env):
    # Prepare configuration sources
    config_source1 = {"name": "${USER}", "age": "@int 30"}
    config_source2 = {"model": "SVM", "num_iter": "@int 35"}
    config_source3 = lambda: {"processor": "add_one", "seq": "@tuple (1,2,3)"}
    config_source4 = lambda: {"model_A": "@float @resolve age"}
    config_source5 = {"dependency_paths": ["path/ok"]}

    # Create a SimpleConfigurationLoader instance
    loader = SimpleConfigurationLoader(
        config_source1,
        config_source2,
        config_source3,
        config_source4,
        config_source5,
        include_env=False,  # We set include_env to False to control the environment in tests
    )

    # Load the configuration
    config = loader.load()

    # Test configuration values
    assert config.name == "test_user"
    assert config.age == 30
    assert isinstance(config.age, int)
    assert config.model == "SVM"
    assert config.num_iter == 35
    assert config.processor == "add_one"
    assert config.seq == (1, 2, 3)
    assert config.model_A == 30.0
    assert isinstance(config.model_A, float)
    assert config.dependency_paths == ["path/ok"]


def test_custom_converters():
    # Add a custom converter
    def multiply_by_two(value: str) -> int:
        return int(value) * 2

    add_custom_converter("double", multiply_by_two)
    assert "double" in _CONVERTERS

    config_source = {"value": "@double 21"}
    loader = SimpleConfigurationLoader(config_source, include_env=False)
    config = loader.load()

    assert config.value == 42

    # Clean up by removing the custom converter
    _CONVERTERS.pop("double")


def test_dependency_injection():
    loader = SimpleConfigurationLoader(
        {"db_url": "sqlite:///:memory:"}, include_env=False
    )
    context = Context(loader)

    @context.dependency()
    def db_connection(configuration):
        return f"Connected to {configuration.db_url}"

    @context.dependency()
    def repository(db_connection):
        return f"Repository using {db_connection}"

    @context
    def business_logic(repository):
        return f"Business logic with {repository}"

    result = business_logic()
    assert (
        result == "Business logic with Repository using Connected to sqlite:///:memory:"
    )


def test_singleton_dependency():
    loader = SimpleConfigurationLoader(include_env=False)
    context = Context(loader)
    counter = {"count": 0}

    @context.dependency(singleton=True)
    def singleton_service():
        counter["count"] += 1
        return f"Service instance {counter['count']}"

    # Access the singleton dependency multiple times
    instance1 = context.get("singleton_service")
    instance2 = context.get("singleton_service")

    assert instance1 == instance2
    assert counter["count"] == 1  # The factory should be called only once


def test_non_singleton_dependency():
    loader = SimpleConfigurationLoader(include_env=False)
    context = Context(loader)
    counter = {"count": 0}

    @context.dependency(singleton=False)
    def transient_service():
        counter["count"] += 1
        return f"Service instance {counter['count']}"

    # Access the transient dependency multiple times
    instance1 = context.get("transient_service")
    instance2 = context.get("transient_service")

    assert instance1 != instance2
    assert counter["count"] == 2  # The factory should be called twice


def test_namespaced_dependencies():
    loader = SimpleConfigurationLoader(include_env=False)
    parent_context = Context(loader, namespace="parent")
    child_context = Context(loader, namespace="child", parent=parent_context)

    @parent_context.dependency()
    def service():  # type: ignore
        return "Service from parent"

    @child_context.dependency()
    def service():
        return "Service from child"

    # Access dependencies
    assert child_context.get("service") == "Service from child"
    assert parent_context.get("service") == "Service from parent"
    assert child_context.get("service", namespace="parent") == "Service from parent"


def test_async_dependency():
    loader = SimpleConfigurationLoader(include_env=False)
    context = Context(loader)

    @context.dependency()
    async def async_service():
        await asyncio.sleep(0.1)
        return "Async Service"

    # Access the asynchronous dependency
    result = context.get("async_service")
    assert result == "Async Service"


def test_dependency_cycle_detection():
    loader = SimpleConfigurationLoader(include_env=False)
    context = Context(loader)

    @context.dependency()
    def service_a(service_b):
        return "Service A"

    @context.dependency()
    def service_b(service_a):
        return "Service B"

    with pytest.raises(DependencyCycleError) as exc_info:
        context.get("service_a")

    assert "Dependency cycle detected" in str(exc_info.value)


def test_dependency_not_found():
    loader = SimpleConfigurationLoader(include_env=False)
    context = Context(loader)

    with pytest.raises(DependencyNotFoundError) as exc_info:
        context.get("nonexistent_dependency")

    assert "Dependency 'nonexistent_dependency' not found" in str(exc_info.value)


def test_plugin_loading(tmp_path):
    # Create a temporary plugin directory
    plugin_dir = tmp_path / "plugins"
    plugin_dir.mkdir()

    # Create a plugin file
    plugin_file = plugin_dir / "my_plugin.py"
    plugin_file.write_text(
        """
from cdf.core.context import dependency

@dependency()
def plugin_service():
    return "Service from plugin"
"""
    )

    # Prepare configuration with plugin paths
    config_source = {"dependency_paths": [str(plugin_dir)]}
    loader = SimpleConfigurationLoader(config_source, include_env=False)
    context = Context(loader)

    # Load plugins
    context.load_dependencies_from_config()

    # Access the service provided by the plugin
    assert context.get("plugin_service") == "Service from plugin"


def test_context_combining():
    loader1 = SimpleConfigurationLoader({"name": "Context1"}, include_env=False)
    context1 = Context(loader1)

    @context1.dependency()
    def service_a():
        return "Service A from Context1"

    loader2 = SimpleConfigurationLoader({"age": 42}, include_env=False)
    context2 = Context(loader2)

    @context2.dependency()
    def service_b():
        return "Service B from Context2"

    combined_context = context1.combine(context2)

    # Configurations should be merged
    assert combined_context.config.name == "Context1"
    assert combined_context.config.age == 42

    # Dependencies should be available from both contexts
    assert combined_context.get("service_a") == "Service A from Context1"
    assert combined_context.get("service_b") == "Service B from Context2"


def test_environment_variable_expansion(monkeypatch):
    monkeypatch.setenv("APP_ENV", "production")
    config_source = {"env": "${APP_ENV}"}
    loader = SimpleConfigurationLoader(config_source, include_env=False)
    config = loader.load()
    assert config.env == "production"


def test_reload_config():
    config_source = {"value": 1}
    loader = SimpleConfigurationLoader(config_source, include_env=False)
    context = Context(loader)
    assert context.config.value == 1

    # Modify the configuration source
    config_source["value"] = 2

    # Reload the configuration
    context.reload_config()
    assert context.config.value == 2


def test_converter_box():
    config_source = {
        "list_value": "@list [1,2,3]",
        "dict_value": "@dict {'a':1,'b':2}",
        "bool_value": "@bool True",
    }
    loader = SimpleConfigurationLoader(config_source, include_env=False)
    config = loader.load()

    assert config.list_value == [1, 2, 3]
    assert config.dict_value == {"a": 1, "b": 2}
    assert config.bool_value is True


def test_expand_env_vars(monkeypatch):
    monkeypatch.setenv("MY_VAR", "hello")
    from cdf.core.context import _expand_env_vars

    template = "Value is $MY_VAR"
    result = _expand_env_vars(template)
    assert result == "Value is hello"


def test_async_injection():
    loader = SimpleConfigurationLoader(include_env=False)
    context = Context(loader)

    @context.dependency()
    async def async_dependency():
        await asyncio.sleep(0.1)
        return "Async Dependency"

    @context
    async def async_function(async_dependency):
        return f"Received {async_dependency}"

    import aiomonitor

    loop = asyncio.new_event_loop()
    with aiomonitor.start_monitor(loop=loop):
        result = loop.run_until_complete(async_function())
        assert result == "Received Async Dependency"


def test_dependency_with_parameters():
    loader = SimpleConfigurationLoader({"db_host": "localhost"}, include_env=False)
    context = Context(loader)

    @context.dependency()
    def db_connection(configuration):
        host = configuration.db_host
        return f"Connected to DB at {host}"

    assert context.get("db_connection") == "Connected to DB at localhost"


def test_dependency_removal():
    loader = SimpleConfigurationLoader(include_env=False)
    context = Context(loader)

    @context.dependency()
    def temp_service():
        return "Temporary Service"

    assert context.get("temp_service") == "Temporary Service"

    # Remove the dependency
    del context["temp_service"]

    with pytest.raises(DependencyNotFoundError):
        context.get("temp_service")


def test_dependency_overriding():
    loader = SimpleConfigurationLoader(include_env=False)
    parent_context = Context(loader, namespace="parent")
    child_context = Context(loader, namespace="child", parent=parent_context)

    @parent_context.dependency()
    def service():  # type: ignore
        return "Service from parent"

    @child_context.dependency()
    def service():  # type: ignore
        return "Service from child"

    # Override the dependency in the child context
    child_context.add("service", "Overridden Service")

    assert child_context.get("service") == "Overridden Service"


def test_context_manager():
    loader = SimpleConfigurationLoader(include_env=False)
    context = Context(loader)

    with context:
        active_ctx = active_context.get()
        assert active_ctx is context

    with pytest.raises(LookupError):
        active_context.get()


def test_dependency_injection_with_context():
    loader = SimpleConfigurationLoader(include_env=False)
    context = Context(loader)

    @context
    def function_with_context(context):
        return context

    result = function_with_context()
    assert result is context


def test_dependency_injection_with_configuration():
    loader = SimpleConfigurationLoader({"setting": "value"}, include_env=False)
    context = Context(loader)

    @context
    def function_with_configuration(configuration):
        return configuration.setting

    result = function_with_configuration()
    assert result == "value"


def test_dependency_injection_with_missing_dependency():
    loader = SimpleConfigurationLoader(include_env=False)
    context = Context(loader)

    @context
    def function_with_missing_dep(missing_dep):
        pass

    with pytest.raises(TypeError, match="missing 1 required positional argument"):
        function_with_missing_dep()


def test_dependency_injection_with_default():
    loader = SimpleConfigurationLoader(include_env=False)
    context = Context(loader)

    @context
    def function_with_default(missing_dep=None):
        return missing_dep

    result = function_with_default()
    assert result is None


def test_dependency_injection_with_various_parameters():
    loader = SimpleConfigurationLoader({"value": 42}, include_env=False)
    context = Context(loader)

    @context
    def function(a, b=2, *, c, d=4, configuration):
        return a, b, c, d, configuration.value

    result = function(1, c=3)
    assert result == (1, 2, 3, 4, 42)


def test_dependency_cycle_with_parent():
    loader = SimpleConfigurationLoader(include_env=False)
    parent_context = Context(loader)
    child_context = Context(loader, parent=parent_context)

    @parent_context.dependency()
    def service_a(service_b):
        return "Service A"

    @child_context.dependency()
    def service_b(service_a):
        return "Service B"

    with pytest.raises(TypeError, match="missing 1 required positional argument"):
        child_context.get("service_a")


def test_plugin_loading_with_nonexistent_path():
    loader = SimpleConfigurationLoader(
        {"dependency_paths": ["nonexistent/path"]}, include_env=False
    )
    context = Context(loader)

    with pytest.raises(ValueError) as exc_info:
        context.load_dependencies_from_config()

    assert "is not a directory or does not exist" in str(exc_info.value)


def test_dependency_with_namespace():
    loader = SimpleConfigurationLoader(include_env=False)
    context = Context(loader, namespace="main")

    @context.dependency(namespace="db")
    def db_service():
        return "DB Service"

    assert context.get("db_service", namespace="db") == "DB Service"


def test_combined_context_namespace():
    loader1 = SimpleConfigurationLoader({"name": "Context1"}, include_env=False)
    context1 = Context(loader1, namespace="ns1")

    @context1.dependency()
    def service():  # type: ignore
        return "Service from ns1"

    loader2 = SimpleConfigurationLoader({"age": 42}, include_env=False)
    context2 = Context(loader2, namespace="ns2")

    @context2.dependency()
    def service():  # type: ignore
        return "Service from ns2"

    combined_context = context1.combine(context2)

    assert combined_context.get("service", namespace="ns1") == "Service from ns1"
    assert combined_context.get("service", namespace="ns2") == "Service from ns2"


def test_dependency_with_overridden_namespace():
    loader = SimpleConfigurationLoader(include_env=False)
    context = Context(loader, namespace="default")

    @context.dependency(namespace="ns1")
    def service():
        return "Service from ns1"

    assert context.get("service", namespace="ns1") == "Service from ns1"
    with pytest.raises(DependencyNotFoundError):
        context.get("service")  # Namespace 'default' does not have 'service'


def test_dependency_removal_with_namespace():
    loader = SimpleConfigurationLoader(include_env=False)
    context = Context(loader, namespace="ns")

    @context.dependency()
    def service():
        return "Service"

    assert context.get("service") == "Service"

    del context["service"]

    with pytest.raises(DependencyNotFoundError):
        context.get("service")


def test_dependency_injection_with_multiple_contexts():
    loader1 = SimpleConfigurationLoader({"setting": "value1"}, include_env=False)
    context1 = Context(loader1)

    loader2 = SimpleConfigurationLoader({"setting": "value2"}, include_env=False)
    context2 = Context(loader2)

    @context1
    def function(configuration):
        return configuration.setting

    result1 = function()
    assert result1 == "value1"

    with context2:
        # We are bound to context1
        result2 = function()
        assert result2 == "value1"


def test_dependency_with_async_factory():
    loader = SimpleConfigurationLoader(include_env=False)
    context = Context(loader)

    @context.dependency()
    async def async_factory():
        await asyncio.sleep(0.1)
        return "Async Factory Result"

    result = context.get("async_factory")
    assert result == "Async Factory Result"


def test_dependency_with_exception_in_factory():
    loader = SimpleConfigurationLoader(include_env=False)
    context = Context(loader)

    @context.dependency()
    def faulty_factory():
        raise ValueError("Factory Error")

    with pytest.raises(ValueError) as exc_info:
        context.get("faulty_factory")

    assert "Factory Error" in str(exc_info.value)


def test_dependency_with_parameters_and_defaults():
    loader = SimpleConfigurationLoader({"param": "value"}, include_env=False)
    context = Context(loader)

    @context.dependency()
    def param_service(param="default"):
        return f"Param is {param}"

    assert context.get("param_service") == "Param is default"


def test_dependency_with_callable_config_source():
    loader = SimpleConfigurationLoader(lambda: {"key": "value"}, include_env=False)
    config = loader.load()
    assert config.key == "value"


def test_combined_contexts_with_conflicting_dependencies():
    loader1 = SimpleConfigurationLoader(include_env=False)
    context1 = Context(loader1)

    @context1.dependency()
    def service():  # type: ignore
        return "Service from context1"

    loader2 = SimpleConfigurationLoader(include_env=False)
    context2 = Context(loader2)

    @context2.dependency()
    def service():  # type: ignore
        return "Service from context2"

    combined_context = context1.combine(context2)

    # The combined context should use the dependencies from context1
    assert combined_context.get("service") == "Service from context2"


def test_dependency_injection_with_partial_parameters():
    loader = SimpleConfigurationLoader(include_env=False)
    context = Context(loader)

    @context
    def function(a, b, c=3):
        return a + b + c

    result = function(1, 2)
    assert result == 6


def test_context_with_no_namespace():
    loader = SimpleConfigurationLoader(include_env=False)
    context = Context(loader)

    @context.dependency()
    def service():
        return "Service with no namespace"

    assert context.get("service") == "Service with no namespace"


def test_dependency_injection_with_keyword_arguments():
    loader = SimpleConfigurationLoader(include_env=False)
    context = Context(loader)

    @context
    def function(*, a, b):
        return a + b

    result = function(a=1, b=2)
    assert result == 3


def test_dependency_with_overridden_factory():
    loader = SimpleConfigurationLoader(include_env=False)
    context = Context(loader)

    def factory_v1():
        return "Version 1"

    def factory_v2():
        return "Version 2"

    context.add_factory("service", factory_v1)
    assert context.get("service") == "Version 1"

    # Override the factory
    context.add_factory("service", factory_v2)
    assert context.get("service") == "Version 2"


def test_dependency_with_different_singleton_settings():
    loader = SimpleConfigurationLoader(include_env=False)
    context = Context(loader)
    counter = {"count": 0}

    @context.dependency(singleton=False)
    def non_singleton_service():
        counter["count"] += 1
        return counter["count"]

    value1 = context.get("non_singleton_service")
    value2 = context.get("non_singleton_service")
    assert value1 != value2

    @context.dependency(singleton=True)
    def singleton_service():
        counter["count"] += 1
        return counter["count"]

    value3 = context.get("singleton_service")
    value4 = context.get("singleton_service")
    assert value3 == value4


def test_dependency_injection_with_varargs():
    loader = SimpleConfigurationLoader(include_env=False)
    context = Context(loader)

    @context
    def function(*args):
        return args

    result = function(1, 2, 3)
    assert result == (1, 2, 3)


def test_dependency_with_complex_namespace():
    loader = SimpleConfigurationLoader(include_env=False)
    context = Context(loader, namespace="main")

    @context.dependency(namespace="ns1")
    def service_ns1():
        return "Service in ns1"

    @context.dependency(namespace="ns2")
    def service_ns2():
        return "Service in ns2"

    assert context.get("service_ns1", namespace="ns1") == "Service in ns1"
    assert context.get("service_ns2", namespace="ns2") == "Service in ns2"


def test_dependency_with_callable_instance():
    loader = SimpleConfigurationLoader(include_env=False)
    context = Context(loader)

    class Service:
        def __call__(self):
            return "Callable Service"

    service_instance = Service()
    context.add("service", service_instance)

    assert context.get("service")() == "Callable Service"
