"""The context module provides thread-safe context variables and injection mechanisms."""

import typing as t
from contextvars import ContextVar

import dlt
from dlt.common.configuration.container import Container
from dlt.common.configuration.providers import ConfigProvider
from dlt.common.configuration.specs.config_providers_context import (
    ConfigProvidersContext,
)
from dlt.common.destination import TDestinationReferenceArg

if t.TYPE_CHECKING:
    from cdf.core.project import ContinuousDataFramework


active_project: ContextVar["ContinuousDataFramework"] = ContextVar("active_project")
"""The active project context variable."""

debug_mode: ContextVar[bool] = ContextVar("debug_mode", default=False)
"""The debug mode context variable."""

T = t.TypeVar("T")


def _ident(x: T) -> T:
    return x


class ExecutionContext(t.NamedTuple):
    """The execution context passed from the CLI."""

    pipeline_name: str
    """The pipeline name."""

    dataset_name: str
    """The dataset name."""
    destination: TDestinationReferenceArg
    """The destination."""
    staging: t.Optional[TDestinationReferenceArg] = None
    """The staging location."""

    select: t.Optional[t.List[str]] = None
    """A list of glob patterns to select resources."""
    exclude: t.Optional[t.List[str]] = None
    """A list of glob patterns to exclude resources."""

    force_replace: bool = False
    """Whether to force replace disposition."""
    intercept_sources: t.Optional[t.Set[dlt.sources.DltSource]] = None
    """Stores the intercepted sources in itself if provided."""
    enable_stage: bool = True
    """Whether to stage data if a staging location is provided."""

    applicator: t.Callable[[dlt.sources.DltSource], dlt.sources.DltSource] = _ident
    """The transformation to apply to the sources."""


execution_context: ContextVar[ExecutionContext] = ContextVar("execution_context")


class CDFConfigProvider(ConfigProvider):
    """A configuration provider for CDF settings."""

    def __init__(self, config: "ContinuousDataFramework") -> None:
        self._config = config
        super().__init__()

    def get_value(
        self, key: str, hint: t.Type[t.Any], pipeline_name: str, *sections: str
    ) -> t.Tuple[t.Optional[t.Any], str]:
        if pipeline_name:
            sections = ("pipelines", pipeline_name, "options", *sections)
        fqn = ".".join((*sections, key))
        try:
            return self._config[fqn], fqn
        except KeyError:
            return None, fqn

    def set_value(
        self, key: str, value: t.Any, pipeline_name: str, *sections: str
    ) -> None:
        import dynaconf

        if pipeline_name:
            sections = ("pipelines", pipeline_name, "options", *sections)
        fqn = ".".join((*sections, key))
        if isinstance(value, dynaconf.Dynaconf):
            if key is None:
                self._config.configuration.maps[0] = value
            else:
                self._config.configuration[fqn].update(value)
        else:
            if key is None:
                raise ValueError("cdf config provider must contain dynaconf settings")
            if isinstance(value, dict):
                value = {k: v for k, v in value.items() if v is not None}
                if isinstance(self._config.configuration.get(fqn), dict):
                    self._config.configuration[fqn].update(value)
                    return
            self._config.configuration[fqn] = value

    @property
    def name(self) -> str:
        return "cdf_config_provider"

    @property
    def supports_sections(self) -> bool:
        return True

    @property
    def supports_secrets(self) -> bool:
        return True

    @property
    def is_empty(self) -> bool:
        return False

    @property
    def is_writable(self) -> bool:
        return True


def inject_cdf_config_provider(cdf: "ContinuousDataFramework") -> None:
    """Injects CDFConfigProvider into the ConfigProvidersContext.

    Args:
        config: The configuration to inject
    """
    Container()[ConfigProvidersContext].add_provider(CDFConfigProvider(cdf))
