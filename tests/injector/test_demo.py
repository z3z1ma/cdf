from __future__ import annotations

import abc
from typing import Any

import cdf.injector
from typing_extensions import override


def get_db_value(_0: Any, _1: Any) -> bool:
    return False


class Seat:
    pass


class Engine(abc.ABC):
    @property
    @abc.abstractmethod
    def started(self) -> bool: ...

    @abc.abstractmethod
    def start(self) -> None: ...


class DBEngine(Engine, cdf.injector.SingletonMixin):
    def __init__(self, db_address: str) -> None:
        self.db_address = db_address

    @property
    @override
    def started(self) -> bool:
        return get_db_value(self.db_address, "engine")

    @override
    def start(self) -> None:
        pass


class MockEngine(Engine, cdf.injector.SingletonMixin):
    @property
    @override
    def started(self) -> bool:
        return True

    @override
    def start(self) -> None:
        pass


class Car(cdf.injector.SingletonMixin):
    def __init__(self, seats: list[Seat], engine: Engine) -> None:
        self.seats = seats
        self.engine = engine

        self.state = 0

    def drive(self) -> None:
        if not self.engine.started:
            self.engine.start()
        self.state = 1

    def stop(self) -> None:
        self.state = 0


class EngineConfig(cdf.injector.Config):
    db_address = cdf.injector.GlobalInput(type_=str, default="ava-db")
    engine = DBEngine(db_address)


class CarConfig(cdf.injector.Config):
    engine_config = EngineConfig()

    seat_cls = cdf.injector.Object(Seat)
    seats = cdf.injector.Prototype(
        lambda cls, n: [cls() for _ in range(n)], seat_cls, 2
    )

    car = Car(seats, engine=engine_config.engine)


def test_basic_demo() -> None:
    config = cdf.injector.get_config(CarConfig, db_address="ava-db")
    container = cdf.injector.get_container(config)

    car: Car = container.config.car
    assert isinstance(car, Car)
    assert id(car) == id(container.config.car)  # Because it's a Singleton
    assert isinstance(car.engine, DBEngine)


def test_perturb_demo() -> None:
    config = cdf.injector.get_config(CarConfig, db_address="ava-db")
    config.engine_config.engine = MockEngine()  # type: ignore
    container = cdf.injector.get_container(config)

    assert isinstance(container.config.car.engine, MockEngine)
