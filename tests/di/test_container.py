# mypy: disable-error-code="comparison-overlap"
from __future__ import annotations

from typing import Any, TypeVar, cast

import cdf.di
import pytest

from . import test_config

TC = TypeVar("TC", bound=cdf.di.Config)


def get_container_objs(
    config: TC | type[TC], more_type_safe: bool, **global_inputs: Any
) -> tuple[cdf.di.Container[TC], TC]:
    if more_type_safe:
        if not isinstance(config, cdf.di.Config):
            config = cdf.di.get_config(config, **global_inputs)
        elif global_inputs:
            raise ValueError("Cannot set config obj and global inputs")

        container = cdf.di.get_container(config)

        config_proxy = container.config
    else:
        if not isinstance(config, cdf.di.Config):
            config = config().get(**global_inputs)
        elif global_inputs:
            raise ValueError("Cannot set config obj and global inputs")

        assert isinstance(config, cdf.di.Config)
        container = cdf.di.Container(config)

        config_proxy = container  # type: ignore

    # Cast because container will act like TC
    return cast(cdf.di.Container[TC], container), cast(TC, config_proxy)


@pytest.mark.parametrize("more_type_safe", [True, False])
def test_basic(more_type_safe: bool) -> None:
    _, config_proxy = get_container_objs(
        test_config.BasicConfig, more_type_safe=more_type_safe
    )

    assert config_proxy.x == 1
    assert config_proxy.y == 2
    assert isinstance(config_proxy.foo, test_config.SingletonValueWrapper)
    assert config_proxy.foo.value == 1
    assert isinstance(config_proxy.bar, test_config.PrototypeValueWrapper)
    assert config_proxy.bar.value == 2

    assert config_proxy.foo is config_proxy.foo  # foo is a Singleton
    assert config_proxy.bar is not config_proxy.bar  # foo is a Prototype


@pytest.mark.parametrize("more_type_safe", [True, False])
def test_perturb_basic(more_type_safe: bool) -> None:
    config = test_config.get_config(
        test_config.BasicConfig, more_type_safe=more_type_safe
    )

    config.x = 2

    _, config_proxy = get_container_objs(config, more_type_safe=more_type_safe)
    assert config_proxy.y == 3
    assert config_proxy.foo.value == 2
    assert config_proxy.bar.value == 3


@pytest.mark.parametrize("more_type_safe", [True, False])
def test_get_nested(more_type_safe: bool) -> None:
    container, config_proxy = get_container_objs(
        test_config.GrandParentConfig, more_type_safe=more_type_safe
    )

    assert config_proxy.parent_config0.basic_config.x == 1
    assert container["parent_config0.basic_config.x"] == 1


@pytest.mark.parametrize("more_type_safe", [True, False])
def test_dir(more_type_safe: bool) -> None:
    container, config_proxy = get_container_objs(
        test_config.GrandParentConfig, more_type_safe=more_type_safe
    )

    assert (
        dir(container)
        == dir(config_proxy)
        == [
            "foobar",
            "parent_config0",
            "parent_config1",
            "some_str0",
        ]
    )
    assert dir(config_proxy.parent_config0) == ["basic_config", "baz0"]
    assert dir(container["parent_config0"]) == ["basic_config", "baz0"]


@pytest.mark.parametrize("more_type_safe", [True, False])
def test_perturb_nested(more_type_safe: bool) -> None:
    config = test_config.get_config(
        test_config.GrandParentConfig, more_type_safe=more_type_safe
    )

    config.parent_config0.basic_config.x = 10

    _, config_proxy = get_container_objs(config, more_type_safe=more_type_safe)

    assert config_proxy.parent_config0.basic_config.x == 10
    assert config_proxy.parent_config0.basic_config.foo.value == 10
    assert config_proxy.parent_config0.basic_config.bar.value == 11
    assert (
        config_proxy.parent_config0.basic_config.foo
        is config_proxy.parent_config1.basic_config.foo
    )
    assert config_proxy.parent_config0.baz0.value == 10
    assert config_proxy.parent_config1.baz1.value == 10
    assert config_proxy.foobar.value == 10


class MoreComplexPerturbConfig0(cdf.di.Config):
    x: int = cdf.di.GlobalInput(type_=int)
    y: int = cdf.di.Object(2)

    foo = cdf.di.Singleton(test_config.ValueWrapper, 100)


class MoreComplexPerturbConfig1(cdf.di.Config):
    other_config = MoreComplexPerturbConfig0()

    value_obj = cdf.di.Singleton(test_config.ValueWrapper, other_config.x)


class MoreComplexPerturbConfig2(cdf.di.Config):
    other_config = MoreComplexPerturbConfig1()

    value_obj = cdf.di.Singleton(test_config.ValueWrapper, other_config.other_config.x)


class Doubler:
    def __init__(self, value: int) -> None:
        self.value = value * 2


@pytest.mark.parametrize("more_type_safe", [True, False])
def test_perturb_nested_more_complex_input(more_type_safe: bool) -> None:
    config = test_config.get_config(
        MoreComplexPerturbConfig1, more_type_safe=more_type_safe, x=100
    )

    config.value_obj = cdf.di.Singleton(  # type: ignore
        Doubler,
        config.other_config.x,
    )

    _, config_proxy = get_container_objs(config, more_type_safe=more_type_safe)

    assert config_proxy.value_obj.value == 200


@pytest.mark.parametrize("more_type_safe", [True, False])
def test_perturb_nested_more_complex_object0(more_type_safe: bool) -> None:
    config = test_config.get_config(
        MoreComplexPerturbConfig1, more_type_safe=more_type_safe, x=100
    )

    config.value_obj = cdf.di.Singleton(  # type: ignore
        Doubler,
        config.other_config.y,
    )

    _, config_proxy = get_container_objs(config, more_type_safe=more_type_safe)

    assert config_proxy.value_obj.value == 4


@pytest.mark.parametrize("more_type_safe", [True, False])
def test_perturb_nested_more_complex_object1(more_type_safe: bool) -> None:
    config = test_config.get_config(
        MoreComplexPerturbConfig2, more_type_safe=more_type_safe, x=100
    )

    config.value_obj = cdf.di.Singleton(  # type: ignore
        Doubler, config.other_config.other_config.y
    )

    _, config_proxy = get_container_objs(config, more_type_safe=more_type_safe)

    assert config_proxy.value_obj.value == 4


@pytest.mark.parametrize("more_type_safe", [True, False])
def test_nested_keyerror(more_type_safe: bool) -> None:
    _, config_proxy = get_container_objs(
        test_config.ErrorGrandParentConfig, more_type_safe=more_type_safe
    )

    with pytest.raises(KeyError):
        try:
            _ = config_proxy.foobar
        except KeyError as exc:
            assert str(exc) == (
                "\"<class 'tests.di.test_config.ParentConfig0'>: "
                "'non_existent_field'\""
            )
            raise


@pytest.mark.parametrize("more_type_safe", [True, False])
def test_input_config(more_type_safe: bool) -> None:
    config = test_config.get_config(
        test_config.InputConfig1, more_type_safe=more_type_safe, name="hi"
    )

    config.input_config0.x = 100

    _, config_proxy = get_container_objs(config, more_type_safe=more_type_safe)
    assert config_proxy.input_config0.name == "hi"
    assert config_proxy.input_config0.context == "default"
    assert config_proxy.input_config0.x == 100
    assert config_proxy.y == 101


@pytest.mark.parametrize("more_type_safe", [True, False])
def test_collection_config(more_type_safe: bool) -> None:
    _, config_proxy = get_container_objs(
        test_config.CollectionConfig, more_type_safe=more_type_safe
    )

    assert config_proxy.x == 1
    assert config_proxy.y == 2
    assert config_proxy.foo_tuple == (1, 2)
    assert config_proxy.foo_list == [1, 2]
    assert config_proxy.foo_dict_kwargs == {"x": 1, "y": 2}
    assert config_proxy.foo_dict_values0 == {1: 1, 2: 2}
    assert config_proxy.foo_dict_values1 == {"values": 1}
    assert config_proxy.foo_dict_values2 == {"x": 1, "y": 2, "z": 3}

    assert config_proxy.foo_tuple is config_proxy.foo_tuple


@pytest.mark.parametrize("more_type_safe", [True, False])
def test_anonymous(more_type_safe: bool) -> None:
    _, config_proxy = get_container_objs(
        test_config.AnonymousConfig, more_type_safe=more_type_safe
    )

    assert config_proxy.y.value.value is config_proxy.x
    assert config_proxy.z.value.value is config_proxy.x


@pytest.mark.parametrize("more_type_safe", [True, False])
def test_underscore(more_type_safe: bool) -> None:
    _, config_proxy = get_container_objs(
        test_config.WrapperConfig, more_type_safe=more_type_safe
    )

    assert config_proxy.value.value is config_proxy._value


@pytest.mark.parametrize("more_type_safe", [True, False])
def test_forward(more_type_safe: bool) -> None:
    _, config_proxy = get_container_objs(
        test_config.ForwardConfig, more_type_safe=more_type_safe
    )

    assert config_proxy.foo is config_proxy.other_config.parent_config0.basic_config.foo
    assert (
        config_proxy.foo_value.value
        is config_proxy.other_config.parent_config0.basic_config.foo
    )


@pytest.mark.parametrize("more_type_safe", [True, False])
def test_perturb_forward(more_type_safe: bool) -> None:
    config = test_config.get_config(
        test_config.ForwardConfig, more_type_safe=more_type_safe
    )

    config.x = 1000

    _, config_proxy = get_container_objs(config, more_type_safe=more_type_safe)

    # Objs that depend on original x remain unperturbed, but objs
    # that depend on the forward alias are perturbed.
    assert config_proxy.other_config.parent_config0.basic_config.x == 1
    assert config_proxy.other_config.foobar.value == 1
    assert config_proxy.x == 1000
    assert config_proxy.x_value.value == 1000


@pytest.mark.parametrize("more_type_safe", [True, False])
def test_perturb_partial_kwargs(more_type_safe: bool) -> None:
    config = test_config.get_config(
        test_config.PartialKwargsConfig, more_type_safe=more_type_safe
    )

    config.x = 10
    config.y = 20

    _, config_proxy = get_container_objs(config, more_type_safe=more_type_safe)

    assert config_proxy.x == 10
    assert config_proxy.y == 20
    assert config_proxy.values.x == 10
    assert config_proxy.values.y == 20
    assert config_proxy.values.z == 10


@pytest.mark.parametrize("more_type_safe", [True, False])
def test_perturb_partial_kwargs_other(more_type_safe: bool) -> None:
    config = test_config.get_config(
        test_config.PartialKwargsOtherConfig, more_type_safe=more_type_safe
    )

    config.partial_kwargs_config.x = 10

    _, config_proxy = get_container_objs(config, more_type_safe=more_type_safe)

    assert config_proxy.partial_kwargs_config.values.x == 10
    assert config_proxy.partial_kwargs_config.values.y == 2
    assert config_proxy.partial_kwargs_config.values.z == 10

    assert config_proxy.values.x == 10
    assert config_proxy.values.y == 2
    assert config_proxy.values.z == 3


class ObjWithAttr:
    @property
    def _test_attr(self) -> int:
        return 1


class ObjAttrConfig(cdf.di.Config):
    test_obj = cdf.di.Singleton(ObjWithAttr)
    test_obj_attr = cdf.di.Forward(test_obj._test_attr)


class NestedConfigObjAttrConfig(cdf.di.Config):
    cfg = ObjAttrConfig()

    test_obj = cdf.di.Forward(cfg.test_obj)
    test_obj_attr = cdf.di.Forward(cfg.test_obj_attr)


@pytest.mark.parametrize("more_type_safe", [True, False])
def test_obj_attr(more_type_safe: bool) -> None:
    _, config_proxy = get_container_objs(ObjAttrConfig, more_type_safe=more_type_safe)

    assert config_proxy.test_obj._test_attr == 1
    assert config_proxy.test_obj_attr == 1


@pytest.mark.parametrize("more_type_safe", [True, False])
def test_nested_config_obj_attr(more_type_safe: bool) -> None:
    _, config_proxy = get_container_objs(
        NestedConfigObjAttrConfig, more_type_safe=more_type_safe
    )

    assert config_proxy.test_obj._test_attr == 1
    assert config_proxy.test_obj_attr == 1
    assert config_proxy.test_obj is config_proxy.cfg.test_obj


def test_typing() -> None:
    config = cdf.di.get_config(test_config.ParentConfig1)

    # Would trigger mypy (and PyCharm, since we're using get_container) error:
    # container: cdf.di.Container[
    #     test_config.BasicConfig,
    # ] = cdf.di.get_container(config)

    container: cdf.di.Container[test_config.ParentConfig1] = cdf.di.get_container(
        config
    )

    # Would trigger mypy error:
    # x: str = container.config.basic_config.x

    x: int = container.config.basic_config.x
    assert x == 1
    y: int = container.config.basic_config.y
    assert y == 2


@pytest.mark.parametrize("more_type_safe", [True, False])
def test_contains(more_type_safe: bool) -> None:
    container, config_proxy = get_container_objs(
        test_config.GrandParentConfig, more_type_safe=more_type_safe
    )

    for exist_key in ["parent_config1", "parent_config1.basic_config.x"]:
        assert exist_key in container
        assert exist_key in config_proxy
        assert exist_key in container._config

    for missing_key in ["foo_bar", "foo_bar.x", "parent_config1.foo_bar"]:
        assert missing_key not in container
        assert missing_key not in config_proxy
        assert missing_key not in container._config


class PerturbSpecConfig(cdf.di.Config):
    foo = cdf.di.Singleton(test_config.ValuesWrapper, 1, 2, z=3)


def test_perturb_spec() -> None:
    config = cdf.di.get_config(PerturbSpecConfig)
    with pytest.raises(cdf.di.PerturbSpecError):
        config.foo.x = 100  # type: ignore[misc]
