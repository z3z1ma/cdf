"""The config module provides a configuration provider for CDF scoped settings.

This allows for the configuration to be accessed and modified in a consistent manner across
the codebase leveraging dlt's configuration provider interface. It also makes all of dlt's
semantics which depend on the configuration providers seamlessly work with CDF's configuration.
"""

import typing as t
from collections import ChainMap
from contextlib import contextmanager

import dynaconf
from dlt.common.configuration.container import Container
from dlt.common.configuration.providers import ConfigProvider as _ConfigProvider
from dlt.common.configuration.providers import EnvironProvider
from dlt.common.configuration.specs.config_providers_context import (
    ConfigProvidersContext,
)
from dlt.common.utils import update_dict_nested


class CdfConfigProvider(_ConfigProvider):
    """A configuration provider for CDF scoped settings."""

    def __init__(self, scope: t.ChainMap[str, t.Any], secret: bool = False) -> None:
        """Initialize the provider.

        Args:
            config: The configuration ChainMap.
        """
        if not isinstance(scope, ChainMap):
            scope = ChainMap(scope)
        self._scope = scope
        self._secret = secret

    def get_value(
        self, key: str, hint: t.Type[t.Any], pipeline_name: str, *sections: str
    ) -> t.Tuple[t.Optional[t.Any], str]:
        """Get a value from the configuration."""
        _ = hint
        if pipeline_name:
            sections = ("pipelines", pipeline_name, "options", *sections)
        parts = (*sections, key)
        fqn = ".".join(parts)

        try:
            return self._scope[fqn], fqn
        except KeyError:
            return None, fqn

    def set_value(
        self, key: str, value: t.Any, pipeline_name: str, *sections: str
    ) -> None:
        """Set a value in the configuration."""
        if pipeline_name:
            sections = ("pipelines", pipeline_name, "options", *sections)
        parts = (*sections, key)
        fqn = ".".join(parts)
        if isinstance(value, dynaconf.Dynaconf):
            if key is None:
                self._scope.maps[-1] = t.cast(dict, value)
            else:
                self._scope.maps[-1][fqn].update(value)
            return None
        else:
            if key is None:
                if isinstance(value, dict):
                    self._scope.update(value)
                    return None
                else:
                    raise ValueError("Cannot set a value without a key")
            this = self._scope
            for key in parts[:-1]:
                if key not in this:
                    this[key] = {}
                this = this[key]
            if isinstance(value, dict) and isinstance(this[parts[-1]], dict):
                update_dict_nested(this[parts[-1]], value)
            else:
                this[parts[-1]] = value

    @property
    def name(self) -> str:
        """The name of the provider"""
        return "CDF Configuration Provider"

    @property
    def supports_sections(self) -> bool:
        """This provider supports sections"""
        return True

    @property
    def supports_secrets(self) -> bool:
        """There is no differentiation between secrets and non-secrets for the cdf provider.

        Nothing is persisted. Data is available in memory and backed by the dynaconf settings object.
        """
        return self._secret

    @property
    def is_writable(self) -> bool:
        """Whether the provider is writable"""
        return True


@t.overload
def get_config_providers(
    scope: t.ChainMap[str, t.Any], /, include_env: bool = False
) -> t.Tuple[CdfConfigProvider, CdfConfigProvider]: ...


@t.overload
def get_config_providers(
    scope: t.ChainMap[str, t.Any], /, include_env: bool = True
) -> t.Tuple[EnvironProvider, CdfConfigProvider, CdfConfigProvider]: ...


def get_config_providers(
    scope: t.ChainMap[str, t.Any], /, include_env: bool = True
) -> t.Union[
    t.Tuple[CdfConfigProvider, CdfConfigProvider],
    t.Tuple[EnvironProvider, CdfConfigProvider, CdfConfigProvider],
]:
    """Get the configuration providers for the given scope."""
    cdf_providers = (
        CdfConfigProvider(scope),
        CdfConfigProvider(scope, secret=True),
    )
    if include_env:
        return (EnvironProvider(), *cdf_providers)
    return cdf_providers


@contextmanager
def inject_configuration(
    scope: t.ChainMap[str, t.Any], /, include_env: bool = True
) -> t.Iterator[t.Mapping[str, t.Any]]:
    """Inject the configuration provider into the context

    This allows dlt.config and dlt.secrets to access the scope configuration. Furthermore
    it makes the scope configuration available throughout dlt where things such as extract,
    normalize, and load settings can be specified.
    """
    ctx = Container()[ConfigProvidersContext]
    prior = ctx.providers.copy()
    ctx.providers = list(get_config_providers(scope, include_env=include_env))
    yield scope
    ctx.providers = prior


__all__ = ["CdfConfigProvider", "get_config_providers", "inject_configuration"]
