import typing as t

from cdf.injector import DependencyRegistry


def test_registry():
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
