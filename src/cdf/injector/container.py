from __future__ import annotations

import typing as t

from typing_extensions import override

from . import config as injector_config
from . import specs as injector_specs
from . import utils as injector_utils

TC = t.TypeVar("TC", bound=injector_config.Config)


class ConfigProxy(t.Generic[TC]):
    """Read-only helper to marshal config values."""

    def __init__(
        self, container: Container[TC], config: injector_config.Config
    ) -> None:
        self.container = container
        self.config = config

    def __getattr__(self, key: str) -> t.Any:
        return self.container._get(self.config, key)

    def __getitem__(self, key: str) -> t.Any:
        return injector_utils.nested_getattr(self, key)

    def __contains__(self, key: str) -> bool:
        return key in self.config

    @override
    def __dir__(self) -> t.Iterable[str]:
        return dir(self.config)


class Container(t.Generic[TC]):
    """Materializes and caches (if necessary) objects based on given config."""

    def __init__(self, config: TC) -> None:
        if isinstance(config, injector_config.ConfigSpec):
            raise ValueError(
                "Expected Config type, got ConfigSpec. "
                "Please call .get() on the config."
            )

        self._config = config

        # Once we pass a config to a container, we can no longer
        # perturb it (as this would require updating container caches)
        self._config.freeze()

        self._instance_cache: dict[str | int, t.Any] = {}

    @property
    def config(self) -> TC:
        """More type-safe alternative to attr access."""
        # Cast because ConfigProxy[TC] will act like TC
        return t.cast(TC, ConfigProxy(self, self._config))

    def _process_arg_spec(
        self, config: injector_config.Config, arg: injector_specs.Spec
    ) -> t.Any:
        if arg.spec_id in config._keys:
            config_key = config._keys[arg.spec_id]
            result = self._get(config, config_key)
        elif isinstance(arg, injector_specs._Callable):
            # Anonymous prototype or singleton
            result = self._materialize_callable_spec(config, arg).instantiate()
        elif isinstance(arg, injector_specs._Instance):
            return arg.obj
        else:
            for child_config in config._child_configs.values():
                if arg.spec_id in child_config._keys:
                    return self._process_arg(child_config, arg)

            raise TypeError(f"Unrecognized arg type: {type(arg)}")

        return result

    def _process_arg(self, config: injector_config.Config, arg: t.Any) -> t.Any:
        if isinstance(arg, injector_specs.Spec):
            return self._process_arg_spec(config, arg)
        elif isinstance(arg, injector_specs.AttrFuture):
            config_key = config._keys[arg.root_spec_id]
            result = self._get(config, config_key)

            for attr in arg.attrs:
                result = getattr(result, attr)
        elif isinstance(arg, (tuple, list)):
            result = type(arg)(self._process_arg(config, elem) for elem in arg)
        elif isinstance(arg, dict):
            result = {k: self._process_arg(config, v) for k, v in arg.items()}
        else:
            result = arg

        return result

    def _materialize_callable_spec(
        self, config: injector_config.Config, spec: injector_specs._Callable
    ) -> injector_specs._Callable:
        """Return Spec copy with materialized args/kwargs."""
        materialized_args = [self._process_arg(config, arg) for arg in spec.args]
        materialized_kwargs = {
            key: self._process_arg(config, arg) for key, arg in spec.kwargs.items()
        }
        if spec.lazy_kwargs:
            materialized_lazy_kwargs = self._process_arg(config, spec.lazy_kwargs)
            materialized_kwargs.update(
                {
                    key: self._process_arg(config, arg)
                    for key, arg in materialized_lazy_kwargs.items()
                }
            )

        return spec.copy_with(*materialized_args, **materialized_kwargs)

    def _get(self, config: injector_config.Config, key: str) -> t.Any:
        """Get instance represented by key in given config."""
        spec = getattr(config, key)
        if isinstance(spec, injector_specs._Instance):
            return spec.obj
        elif isinstance(spec, injector_specs._Singleton):
            try:
                return self._instance_cache[spec.spec_id]
            except KeyError:
                pass

            instance = self._materialize_callable_spec(config, spec).instantiate()
            self._instance_cache[spec.spec_id] = instance
            return instance
        elif isinstance(spec, injector_specs._Prototype):
            return self._materialize_callable_spec(config, spec).instantiate()
        elif isinstance(spec, injector_config.Config):
            return ConfigProxy(self, spec)
        elif isinstance(spec, injector_specs.AttrFuture):
            key = config._keys[spec.root_spec_id]
            obj = self._get(config, key)

            for idx, attr in enumerate(spec.attrs):
                obj = getattr(obj, attr)
                if idx == len(spec.attrs) - 1:
                    return obj

            raise ValueError(
                f"Failed to resolve attr reference: "
                f"spec_id={spec.spec_id}, attrs={spec.attrs}"
            )
        else:
            raise ValueError(
                f"Unrecognized spec type: " f"{type(spec)} with key={key!r}"
            )

    def get(self, key: str, default: t.Any | None = None) -> t.Any:
        """Get materialized object aliased by key, with optional default."""
        if key in dir(self):
            return self[key]

        return default

    def clear(self) -> None:
        """Clear instance cache."""
        self._instance_cache.clear()

    def __getattr__(self, key: str) -> t.Any:
        return self._get(self._config, key)

    def __getitem__(self, key: str) -> t.Any:
        return injector_utils.nested_getattr(self, key)

    def __contains__(self, key: str) -> bool:
        return injector_utils.nested_contains(self._config, key)

    @override
    def __dir__(self) -> t.Iterable[str]:
        return dir(self._config)


def get_container(config: TC) -> Container[TC]:
    """More type-safe alternative to creating container (for PyCharm)."""
    return Container(config)
