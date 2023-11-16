"""CDF config providers.

These are dlt config providers that are opinionated for CDF. They are used to read the config
at the project root of a cdf project and should work from anywhere within the project. Like,
sqlmesh, which settled on a top-level `config.{yml,py}` file, we have settled on a top-level
`cdf_config.toml` file. This file is searched for in the current working directory and all
parent directories up to a maximum depth of 3. The first config provider found is used.
"""
import contextlib
import inspect
import typing as t
from pathlib import Path

import dlt
import dlt.common.configuration.providers as providers
from dlt.common.configuration.container import Container
from dlt.common.configuration.specs.config_providers_context import (
    ConfigProvidersContext,
)

import cdf.core.constants as c

if t.TYPE_CHECKING:
    from cdf.core.workspace import Workspace


class CDFConfigTomlProvider(providers.TomlFileProvider):
    """An opinionated config provider for CDF."""

    def __init__(self, project_dir: str | Path = ".") -> None:
        super().__init__(
            c.CONFIG_FILE, project_dir=str(project_dir), add_global_config=True
        )
        self._name = c.CONFIG_FILE

    @property
    def name(self) -> str:
        return self._name

    @name.setter
    def name(self, value: str) -> None:
        self._name = value

    supports_secrets = False  # type: ignore[assignment]
    is_writable = True  # type: ignore[assignment]


class CDFSecretsTomlProvider(providers.TomlFileProvider):
    """An opinionated secrets provider for CDF."""

    def __init__(self, project_dir: str | Path = ".") -> None:
        super().__init__(
            c.SECRETS_FILE, project_dir=str(project_dir), add_global_config=True
        )
        self._name = c.SECRETS_FILE

    @property
    def name(self) -> str:
        return self._name

    @name.setter
    def name(self, value: str) -> None:
        self._name = value

    supports_secrets = True  # type: ignore[assignment]
    is_writable = True  # type: ignore[assignment]


@t.overload
def config_provider_factory(
    custom_name: str,
    project_dir: str | Path = ".",
    secrets: bool = False,
) -> CDFConfigTomlProvider:
    ...


@t.overload
def config_provider_factory(
    custom_name: str,
    project_dir: str | Path = ".",
    secrets: bool = True,
) -> CDFSecretsTomlProvider:
    ...


def config_provider_factory(
    custom_name: str | None = None,
    project_dir: str | Path = ".",
    secrets: bool = False,
) -> CDFConfigTomlProvider | CDFSecretsTomlProvider:
    """Create a config provider.

    Args:
        name: The name of the config provider.
        project_dir: The project directory to use.
        secrets: Whether the config provider supports secrets.

    Returns:
        The config provider.
    """
    provider = (
        CDFSecretsTomlProvider(project_dir=project_dir)
        if secrets
        else CDFConfigTomlProvider(project_dir=project_dir)
    )
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
            if path.joinpath(c.SECRETS_FILE).exists():
                yield CDFSecretsTomlProvider(project_dir=path)
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
        keys: The keys of the config providers to remove.
    """
    for name in names:
        with contextlib.suppress(KeyError):
            Container()[ConfigProvidersContext].pop(name)


def inject_config_providers_from_workspace(workspace: "Workspace") -> None:
    """Add config providers from a workspace.

    Args:
        workspace: The workspace to add config providers from.
    """
    workspace_cfg = config_provider_factory(
        f"{workspace.namespace}.config", project_dir=workspace.root, secrets=False
    )
    workspace_secrets = config_provider_factory(
        f"{workspace.namespace}.secrets", project_dir=workspace.root, secrets=True
    )
    inject_config_providers([workspace_cfg, workspace_secrets])


def remove_config_providers_from_workspace(workspace: "Workspace") -> None:
    """Remove config providers from a workspace.

    Args:
        workspace: The workspace to remove config providers from.
    """
    remove_config_providers(
        f"{workspace.namespace}.config", f"{workspace.namespace}.secrets"
    )


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
