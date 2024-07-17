# mypy: disable-error-code="comparison-overlap"
from __future__ import annotations

import dataclasses
import types
from typing import Any, TypeVar

import cdf.di
import cdf.di.specs
import pytest

TC = TypeVar("TC", bound=cdf.di.Config)


def get_config(config_cls: type[TC], more_type_safe: bool, **global_inputs: Any) -> TC:
    if more_type_safe:
        return cdf.di.get_config(config_cls, **global_inputs)
    else:
        return config_cls().get(**global_inputs)  # type: ignore[no-any-return]


@dataclasses.dataclass(frozen=True)
class ValueWrapper:
    value: Any


@dataclasses.dataclass(frozen=True)
class ValuesWrapper:
    x: Any
    y: Any
    z: Any


@dataclasses.dataclass(frozen=True)
class SingletonValueWrapper(cdf.di.SingletonMixin, ValueWrapper):
    pass


@dataclasses.dataclass(frozen=True)
class PrototypeValueWrapper(cdf.di.PrototypeMixin, ValueWrapper):
    pass


class BasicConfig(cdf.di.Config):
    x = cdf.di.Object(1)
    y: int = cdf.di.Prototype(lambda x, offset: x + offset, x, offset=1)

    foo = SingletonValueWrapper(value=x)
    bar = PrototypeValueWrapper(value=y)


def test_config_spec() -> None:
    # No inputs
    assert BasicConfig() == BasicConfig()
    assert hash(BasicConfig()) == hash(BasicConfig())

    # Basic inputs
    assert BasicConfig(x=1, y="hi") == BasicConfig(x=1, y="hi")
    assert BasicConfig(x=1, y="hi") != BasicConfig()

    assert hash(BasicConfig(x=1, y="hi")) == hash(BasicConfig(x=1, y="hi"))
    assert hash(BasicConfig(x=1, y="hi")) != hash(BasicConfig())


@pytest.mark.parametrize("more_type_safe", [True, False])
def test_basic(more_type_safe: bool) -> None:
    config = get_config(BasicConfig, more_type_safe=more_type_safe)

    assert config._get_spec("x").obj == 1
    assert isinstance(config._get_spec("y").func_or_type, types.LambdaType)
    assert config._get_spec("foo").func_or_type is SingletonValueWrapper
    assert config._get_spec("bar").func_or_type is PrototypeValueWrapper


@pytest.mark.parametrize("more_type_safe", [True, False])
def test_perturb_basic(more_type_safe: bool) -> None:
    config0: BasicConfig = get_config(BasicConfig, more_type_safe=more_type_safe)

    config0.x = 2
    spec_x = config0._get_spec("x")
    assert isinstance(spec_x, cdf.di.specs._Object)
    assert spec_x.obj == 2

    # Note that there are no class-level interactions, so if we
    # create a new instance, it doesn't have prior perturbations
    config1 = cdf.di.get_config(BasicConfig)

    assert config1._get_spec("x").obj == 1


@pytest.mark.parametrize("more_type_safe", [True, False])
def test_perturb_after_freeze(more_type_safe: bool) -> None:
    config = get_config(BasicConfig, more_type_safe=more_type_safe)

    config.freeze()
    with pytest.raises(cdf.di.FrozenConfigError):
        config.x = 100


@pytest.mark.parametrize("more_type_safe", [True, False])
def test_add_key_after_load(more_type_safe: bool) -> None:
    config = get_config(BasicConfig, more_type_safe=more_type_safe)

    with pytest.raises(cdf.di.NewKeyConfigError):
        config.new_x = 100


class ParentConfig0(cdf.di.Config):
    basic_config = BasicConfig()

    baz0 = SingletonValueWrapper(basic_config.x)


class ParentConfig1(cdf.di.Config):
    basic_config = BasicConfig()

    baz1 = SingletonValueWrapper(basic_config.x)
    some_str1 = cdf.di.Object("abc")


class GrandParentConfig(cdf.di.Config):
    parent_config0 = ParentConfig0()
    parent_config1 = ParentConfig1()

    foobar = SingletonValueWrapper(parent_config0.basic_config.x)
    some_str0 = cdf.di.Object("hi")


class ErrorGrandParentConfig(cdf.di.Config):
    parent_config0 = ParentConfig0()
    parent_config1 = ParentConfig1()

    # This is pointing to a non-existent attr, so we will fail when
    # trying to get foobar via the container.
    foobar = cdf.di.Forward(parent_config0.non_existent_field)


@pytest.mark.parametrize("more_type_safe", [True, False])
def test_dir(more_type_safe: bool) -> None:
    config = get_config(GrandParentConfig, more_type_safe=more_type_safe)

    assert dir(config) == [
        "foobar",
        "parent_config0",
        "parent_config1",
        "some_str0",
    ]
    assert dir(config.parent_config0) == ["basic_config", "baz0"]


@pytest.mark.parametrize("more_type_safe", [True, False])
def test_nested_config(more_type_safe: bool) -> None:
    config = get_config(GrandParentConfig, more_type_safe=more_type_safe)

    assert id(config.parent_config0.basic_config) == id(
        config.parent_config1.basic_config
    )


@pytest.mark.parametrize("more_type_safe", [True, False])
def test_perturb_nested_config_attrs(more_type_safe: bool) -> None:
    config = get_config(GrandParentConfig, more_type_safe=more_type_safe)

    config.some_str0 = "hello"
    config.parent_config0.basic_config.x = 100
    config.parent_config1.some_str1 = "def"

    assert config._get_spec("some_str0").obj == "hello"
    assert config.parent_config1.basic_config._get_spec("x").obj == 100
    assert config.parent_config1._get_spec("some_str1").obj == "def"


@pytest.mark.parametrize("more_type_safe", [True, False])
def test_perturb_nested_config_strs(more_type_safe: bool) -> None:
    config = get_config(GrandParentConfig, more_type_safe=more_type_safe)

    config["some_str0"] = "hello"
    config["parent_config0.basic_config.x"] = 100
    config["parent_config1.some_str1"] = "def"

    assert config["some_str0"].obj == "hello"
    assert config["parent_config1.basic_config.x.obj"] == 100
    assert config["parent_config1.some_str1"].obj == "def"


@pytest.mark.parametrize("more_type_safe", [True, False])
def test_perturb_nested_child_config(more_type_safe: bool) -> None:
    config = get_config(GrandParentConfig, more_type_safe=more_type_safe)

    with pytest.raises(cdf.di.SetChildConfigError):
        config.parent_config0 = ParentConfig1()  # type: ignore


class InputConfig0(cdf.di.Config):
    name = cdf.di.GlobalInput(str)
    context = cdf.di.GlobalInput(str, default="default")
    x = cdf.di.LocalInput(int)


class InputConfig1(cdf.di.Config):
    input_config0 = InputConfig0(x=1)

    y = cdf.di.Prototype(lambda x, offset: x + offset, input_config0.x, offset=1)


class BadInputConfig(cdf.di.Config):
    input_config0 = InputConfig0()  # Note missing inputs


@pytest.mark.parametrize("more_type_safe", [True, False])
def test_input_config(more_type_safe: bool) -> None:
    with pytest.raises(cdf.di.InputConfigError):
        InputConfig1().get()

    with pytest.raises(cdf.di.InputConfigError):
        InputConfig1().get(name=1)

    with pytest.raises(cdf.di.InputConfigError):
        BadInputConfig().get(name="hi")

    config = get_config(InputConfig1, name="hi", more_type_safe=more_type_safe)

    assert config.input_config0._get_spec("name").obj == "hi"
    assert config.input_config0._get_spec("context").obj == "default"
    assert config.input_config0._get_spec("x").obj == 1


class CollectionConfig(cdf.di.Config):
    x = cdf.di.Object(1)
    y = cdf.di.Object(2)
    z = cdf.di.Object(3)

    foo_tuple: tuple[int] = cdf.di.SingletonTuple(x, y)
    foo_list: list[int] = cdf.di.SingletonList(x, y)
    foo_dict_kwargs: dict[str, int] = cdf.di.SingletonDict(x=x, y=y)
    foo_dict_values0: dict[int, int] = cdf.di.SingletonDict({1: x, 2: y})
    foo_dict_values1: dict[str, int] = cdf.di.SingletonDict(values=x)
    foo_dict_values2: dict[str, int] = cdf.di.SingletonDict({"x": x, "y": y}, z=z)

    # Check that untyped values don't trigger mypy errors
    _untyped_foo_tuple = cdf.di.SingletonTuple(x, y)
    _untyped_foo_list = cdf.di.SingletonList(x, y)
    _untyped_foo_dict_kwargs = cdf.di.SingletonDict(x=x, y=y)
    _untyped_foo_dict_values0: dict[int, int] = cdf.di.SingletonDict({1: x, 2: y})


class AnonymousConfig(cdf.di.Config):
    x = cdf.di.Singleton(ValueWrapper, 1)
    y = cdf.di.Singleton(ValueWrapper, cdf.di.Singleton(ValueWrapper, x))
    z = cdf.di.Singleton(ValueWrapper, cdf.di.Prototype(ValueWrapper, x))


class WrapperConfig(cdf.di.Config):
    _value = cdf.di.Singleton(ValueWrapper, 1)
    value = cdf.di.Singleton(ValueWrapper, _value)


class ForwardConfig(cdf.di.Config):
    other_config = GrandParentConfig()

    x = cdf.di.Forward(other_config.parent_config0.basic_config.x)
    x_value = cdf.di.Singleton(ValueWrapper, value=x)

    foo = cdf.di.Forward(other_config.parent_config0.basic_config.foo)
    foo_value = cdf.di.Singleton(ValueWrapper, value=foo)


class PartialKwargsConfig(cdf.di.Config):
    x = cdf.di.Object(1)
    y = cdf.di.Object(2)

    partial_kwargs = cdf.di.SingletonDict(x=x, y=y)

    values = cdf.di.Singleton(  # type: ignore[call-arg]
        ValuesWrapper,
        z=x,
        __lazy_kwargs=partial_kwargs,  # pyright: ignore
    )


class PartialKwargsOtherConfig(cdf.di.Config):
    partial_kwargs_config = PartialKwargsConfig()

    z = cdf.di.Object(3)
    values = cdf.di.Singleton(  # type: ignore[call-arg]
        ValuesWrapper,
        z=z,
        __lazy_kwargs=partial_kwargs_config.partial_kwargs,  # pyright: ignore
    )


def test_extra_global_inputs() -> None:
    with pytest.raises(cdf.di.InputConfigError):
        try:
            InputConfig1().get(name="testing", foobar=123)
        except cdf.di.InputConfigError as exc:
            assert "extra" in str(exc) and "'foobar'" in str(exc)
            raise


class InputConfigWithCollision(cdf.di.Config):
    input_config0 = InputConfig0(x=1)

    # "name" collides with input_config0.name
    name = cdf.di.GlobalInput(str)


def test_global_input_collisions() -> None:
    with pytest.raises(cdf.di.InputConfigError):
        try:
            InputConfigWithCollision().get(name="testing")
        except cdf.di.InputConfigError as exc:
            assert "collision" in str(exc) and "'name'" in str(exc)
            raise


def test_typing() -> None:
    # Would trigger mypy error:
    # cfg0: ParentConfig1 = cdf.di.get_config(ParentConfig0)

    cfg0: ParentConfig1 = cdf.di.get_config(ParentConfig1)

    # Would trigger mypy error:
    # _0: str = cfg0.basic_config.x

    _0: int = cfg0.basic_config.x  # noqa: F841
    _1: int = cfg0.basic_config.y  # noqa: F841
