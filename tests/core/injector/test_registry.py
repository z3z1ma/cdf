import typing as t
from unittest.mock import MagicMock

import pytest

from cdf.core.injector.errors import DependencyCycleError
from cdf.core.injector.registry import (
    Dependency,
    DependencyRegistry,
    Lifecycle,
    TypedKey,
)


def test_registry():
    # A generic test to show some of the API together
    container = DependencyRegistry()
    container.add(("a", int), lambda: 1)
    container.add(("b", int), lambda a: a + 1)
    container.add("obj_proto", object, container.lifecycle.PROTOTYPE)
    container.add("obj_singleton", object)

    def foo(a: int, b: int, c: int = 0) -> int:
        return a + b

    foo_wired = container.wire(foo)

    assert foo_wired() == 3
    assert foo_wired(1) == 3
    assert foo_wired(2) == 4
    assert foo_wired(3, 3) == 6

    assert container.get("obj_proto") is not container.get("obj_proto")
    assert container.get("obj_singleton") is container.get("obj_singleton")

    assert container(foo) == 3

    container.add("c", lambda a, b: a + b, container.lifecycle.PROTOTYPE)

    assert container(foo) == 3

    def bar(a: int, b: int, c: t.Optional[int] = None) -> int:
        if c is None:
            raise ValueError("c is required")
        return a + b + c

    assert container(bar) == 6
    assert container(bar, c=5) == 8

    @container.wire
    def baz(a: int, b: int, c: int = 0) -> int:
        return a + b + c

    assert baz() == 3

@pytest.fixture
def registry():
    return DependencyRegistry()


def test_typed_key_creation():
    key = TypedKey(name="test", type_=int)
    assert key.name == "test"
    assert key.type_ is int


def test_typed_key_equality():
    key1 = TypedKey("name1", int)
    key2 = TypedKey("name1", int)
    assert key1 == key2


def test_typed_key_inequality():
    key1 = TypedKey("name1", int)
    key2 = TypedKey("name2", str)
    assert key1 != key2


def test_typed_key_string_representation():
    key1 = TypedKey("name1", int)
    assert str(key1) == "name1: int"


def test_instance_dependency():
    instance = Dependency.instance(42)
    assert instance.factory == 42
    assert instance.lifecycle.is_instance


def test_singleton_dependency():
    factory = MagicMock(return_value=42)
    singleton_dep = Dependency.singleton(factory)
    assert singleton_dep.lifecycle == Lifecycle.SINGLETON


def test_prototype_dependency():
    factory = MagicMock(return_value=42)
    prototype_dep = Dependency.prototype(factory)
    assert prototype_dep.lifecycle == Lifecycle.PROTOTYPE


def test_apply_function_to_instance():
    dep = Dependency.instance(42)
    new_dep = dep.apply(lambda x: x + 1)
    assert new_dep.factory == 43


def test_apply_wrappers_to_factory():
    factory = MagicMock(return_value=42)
    dep = Dependency.singleton(factory)
    wrapper = MagicMock(side_effect=lambda f: lambda: f() + 1)
    dep_wrapped = dep.apply_wrappers(wrapper)
    assert dep_wrapped.unwrap() == 43


def test_add_and_get_singleton(registry: DependencyRegistry):
    registry.add_singleton("test", object)
    retrieved1 = registry.get("test")
    retrieved2 = registry.get("test")
    assert retrieved1 is retrieved2  # Ensure same instance each time


def test_add_and_get_prototype(registry: DependencyRegistry):
    registry.add_prototype("test", object)
    retrieved1 = registry.get("test")
    retrieved2 = registry.get("test")
    assert retrieved1 is not retrieved2  # Ensure new instance each time


def test_add_and_get_instance(registry: DependencyRegistry):
    registry.add_instance("test", 42)
    retrieved = registry.get("test")
    assert retrieved == 42


def test_wire_function(registry: DependencyRegistry):
    factory = MagicMock(return_value=42)
    registry.add_singleton("test", factory)

    @registry.wire
    def func(test):
        return test

    assert func() == 42


def test_dependency_cycle(registry: DependencyRegistry):
    registry.add("left", lambda right: right)
    registry.add("right", lambda left: left)
    with pytest.raises(DependencyCycleError):
        registry["left"]


def test_contains(registry: DependencyRegistry):
    factory = MagicMock(return_value=42)
    registry.add_singleton("test", factory)
    assert "test" in registry


def test_remove_dependency(registry: DependencyRegistry):
    factory = MagicMock(return_value=42)
    registry.add_singleton("test", factory)
    assert "test" in registry
    registry.remove("test")
    assert "test" not in registry
