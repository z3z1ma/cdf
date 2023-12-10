"""CDF config providers.

These are dlt config providers that are opinionated for CDF. They are used to read the config
at the project root of a cdf project and should work from anywhere within the project. Like,
sqlmesh, which settled on a top-level `config.{yml,py}` file, we have settled on a top-level
`cdf_config.toml` file. This file is searched for in the current working directory and all
parent directories up to a maximum depth of 3. The first config provider found is used.
"""
import contextlib
import inspect
import os
import typing as t
from pathlib import Path

import dlt
import dlt.common.configuration.providers as providers
import tomlkit
from dlt.common.configuration.container import Container
from dlt.common.configuration.specs.config_providers_context import (
    ConfigProvidersContext,
)

import cdf.core.constants as c
from cdf.core.jinja import ENVIRONMENT, JINJA_CONTEXT

if t.TYPE_CHECKING:
    from cdf.core.workspace import Workspace


def read_toml(toml_path: str) -> tomlkit.TOMLDocument:
    if os.path.isfile(toml_path):
        with open(toml_path, "r", encoding="utf-8") as f:
            context = f.read()
            template = ENVIRONMENT.from_string(context)
            f = template.render(**JINJA_CONTEXT, **os.environ)
            return tomlkit.loads(f)
    else:
        return tomlkit.document()


class CDFConfigTomlProvider(providers.TomlFileProvider):
    """An opinionated config provider for CDF."""

    def __init__(self, project_dir: str | Path = ".") -> None:
        self._name = c.CONFIG_FILE
        super().__init__(
            c.CONFIG_FILE, project_dir=str(project_dir), add_global_config=True
        )

    @property
    def name(self) -> str:
        return self._name

    @name.setter
    def name(self, value: str) -> None:
        self._name = value

    _read_toml = staticmethod(read_toml)

    @property
    def supports_secrets(self) -> bool:
        return True

    @property
    def is_writable(self) -> bool:
        return True


def config_provider_factory(
    custom_name: str | None = None, project_dir: str | Path = "."
) -> CDFConfigTomlProvider:
    """Create a config provider.

    Args:
        name: The name of the config provider.
        project_dir: The project directory to use.

    Returns:
        The config provider.
    """
    provider = CDFConfigTomlProvider(project_dir=project_dir)
    if custom_name:
        # Providers require unique names when added to the container
        provider.name = custom_name
    return provider


def find_config_providers(
    search_paths: t.Sequence[str | Path] | str | Path,
    search_cwd: bool = True,
    max_depth: int = 3,
) -> t.Iterator[providers.ConfigProvider]:
    """Find CDF configuration files in the search paths.

    Args:
        search_paths: The paths to search for config files.
        search_cwd: Whether to search the current working directory.
        max_depth: The maximum depth to search.

    Returns:
        An iterator of config providers.
    """
    if isinstance(search_paths, (str, Path)):
        search_paths = [search_paths]
    if search_cwd:
        t.cast(t.List[str], search_paths).insert(0, ".")
    for raw_path in search_paths:
        path, depth = Path(raw_path).expanduser().resolve(), 0
        while depth < max_depth and path.parents:
            if path.joinpath(c.CONFIG_FILE).exists():
                yield CDFConfigTomlProvider(project_dir=path)
            path = path.parent
            depth += 1


def inject_config_providers(
    providers: t.List[providers.ConfigProvider],
    method: t.Literal["append", "prepend"] = "prepend",
) -> None:
    """Extend the global config providers with the given providers.

    Args:
        providers: The providers to extend the global config providers with.
    """
    ctx = Container()[ConfigProvidersContext]
    for provider in providers:
        if provider.name in ctx:
            ctx[provider.name] = provider
            continue
        if method == "append":
            ctx.providers.append(provider)
        else:
            ctx.providers.insert(1, provider)


def remove_config_providers(*names: str) -> None:
    """Remove global config providers by key.

    Args:
        keys: The keys of the config providers to remove. If no keys are provided, all providers are removed.
    """
    if not names:
        names = tuple(Container()[ConfigProvidersContext].keys())
    for name in names:
        with contextlib.suppress(KeyError):
            Container()[ConfigProvidersContext].pop(name)


WORKSPACE_PROVIDER_CACHE: t.Dict[str, providers.ConfigProvider] = {}
"""A cache of config providers keyed by workspace."""


def inject_config_providers_from_workspace(workspace: "Workspace") -> None:
    """Add config providers from a workspace.

    The providers are cached so that they are not recreated on every call. This permits
    persistent mutation of the providers in user code using dlt.config[...] = ... which
    creates a more consistent user interface while allowing the library to be flexible
    in entering and exiting contexts.

    Args:
        workspace: The workspace to add config providers from.
    """
    JINJA_CONTEXT["workspace"] = workspace
    JINJA_CONTEXT["root"] = workspace.root
    if workspace in WORKSPACE_PROVIDER_CACHE:
        workspace_cfg = WORKSPACE_PROVIDER_CACHE[workspace]
    else:
        workspace_cfg = config_provider_factory(
            f"{workspace.namespace}.config", project_dir=workspace.root
        )
    inject_config_providers([workspace_cfg])


def remove_config_providers_from_workspace(workspace: "Workspace") -> None:
    """Remove config providers from a workspace.

    Args:
        workspace: The workspace to remove config providers from.
    """
    remove_config_providers(f"{workspace.namespace}.config")


@contextlib.contextmanager
def with_config_providers_from_workspace(workspace: "Workspace") -> t.Iterator[None]:
    context = Container()[ConfigProvidersContext]
    """Add config providers from a workspace for the duration of the context.

    Args:
        workspace: The workspace to add config providers from.
    """
    existing_providers = context.providers.copy()
    context.clear()
    inject_config_providers_from_workspace(workspace)
    yield
    context.clear()
    context.providers = existing_providers


def populate_fn_kwargs_from_config(
    fn: t.Callable[..., t.Any],
    kwargs: t.Dict[str, t.Any],
    private_attrs: t.Set[str] | None = None,
    config_path: t.List[str] | None = None,
) -> t.Dict[str, t.Any]:
    """Populate kwargs from the config.

    Args:
        kwargs: The kwargs to populate. Mutated in place.
        private_attrs: A set of private attributes to exclude.
        config_path: The path to the config. IE ["ff", "harness"]

    Returns:
        The kwargs supplemented by the config providers.
    """
    if config_path is None:
        config_path = []
    private_attrs = private_attrs or set()
    fn_kwargs = inspect.signature(fn).parameters.keys() - private_attrs
    for k in fn_kwargs:
        if k not in kwargs:
            with contextlib.suppress(KeyError):
                kwargs[k] = dlt.config[".".join([*config_path, k])]
            with contextlib.suppress(KeyError):
                kwargs[k] = dlt.secrets[".".join([*config_path, k])]
    return kwargs
