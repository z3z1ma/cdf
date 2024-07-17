from __future__ import annotations

import abc
from typing import Sequence, TypeVar

import cdf.injector
from typing_extensions import override

T = TypeVar("T")


class BaseMultiplier(abc.ABC):
    @abc.abstractmethod
    def get_result(self) -> int: ...


class SimpleMultiplier(BaseMultiplier):
    def __init__(self, x: int, y: int = 2) -> None:
        self.x = x
        self.y = y

    @override
    def get_result(self) -> int:
        return self.x * self.y


class MockMultipler(BaseMultiplier):
    @override
    def get_result(self) -> int:
        return 42


def add_ints(x: int, y: int) -> int:
    return x + y


def test_typing() -> None:
    spec0: int = cdf.injector.Instance(1)
    spec1: int = cdf.injector.Instance(2)
    spec2: str = cdf.injector.Instance("abc")

    _3: int = cdf.injector.GlobalInput(type_=int)  # noqa: F841
    _4: str = cdf.injector.LocalInput(type_=str)  # noqa: F841
    _5: Sequence[str] = cdf.injector.LocalInput(type_=Sequence[str])  # noqa: F841

    _6: BaseMultiplier = cdf.injector.Prototype(MockMultipler)  # noqa: F841
    _7: BaseMultiplier = cdf.injector.Prototype(SimpleMultiplier, 100)  # noqa: F841
    _8: BaseMultiplier = cdf.injector.Singleton(
        SimpleMultiplier, 100, y=3
    )  # noqa: F841
    _9: SimpleMultiplier = cdf.injector.Singleton(
        SimpleMultiplier, 100, y=3
    )  # noqa: F841

    _10: Sequence = cdf.injector.SingletonList(spec0, spec0)  # noqa: F841
    _11: Sequence[int] = cdf.injector.SingletonList(spec0, spec0)  # noqa: F841
    _12: tuple = cdf.injector.SingletonTuple(spec2, spec2)  # noqa: F841
    # TODO: Support more narrow Tuple types
    # _13: Tuple[str, str] = cdf.injector.SingletonTuple(spec2, spec2)  # noqa: F841
    _14: dict = cdf.injector.SingletonDict(a=spec0, b=spec1)  # noqa: F841
    _15: dict[str, int] = cdf.injector.SingletonDict(a=spec0, b=spec1)  # noqa: F841

    # Would cause mypy error:
    # _16: str = cdf.injector.Singleton(add_ints, 1, "abc")  # noqa: F841
    _16: int = cdf.injector.Singleton(add_ints, 1, 2)  # noqa: F841
