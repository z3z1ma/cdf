from cdf.injector.registry import DependencyRegistry

r = DependencyRegistry()
r.add("a", lambda: 1, r.lc.SINGLETON)
r.add("b", lambda a: a + 1, r.lc.SINGLETON)
r.add("obj_proto", object, r.lc.PROTOTYPE)
r.add("obj_singleton", object, r.lc.SINGLETON)


def foo(a: int, b: int, c: int = 0) -> int:
    return a + b


foo_wired = r.wire(foo)

assert foo_wired() == 3
assert foo_wired(1) == 3
assert foo_wired(2) == 4
assert foo_wired(3, 3) == 6

assert r.get("obj_proto") is not r.get("obj_proto")
assert r.get("obj_singleton") is r.get("obj_singleton")

assert r(foo) == 3
