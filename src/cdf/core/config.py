"""CDF config providers.

These are dlt config providers that are opinionated for CDF. They are used to read the config
at the project root of a cdf project and should work from anywhere within the project. Like,
sqlmesh, which settled on a top-level `config.{yml,py}` file, we have settled on a top-level
`cdf_config.toml` file. This file is searched for in the current working directory and all
parent directories up to a maximum depth of 3. The first config provider found is used.
"""
import contextlib
import typing as t
from pathlib import Path

import dlt.common.configuration.exceptions as dlt_e
import dlt.common.configuration.providers as providers
from dlt.common.configuration.container import Container
from dlt.common.configuration.specs.config_providers_context import (
    ConfigProvidersContext,
)

import cdf.core.constants as c
from cdf.core.utils import do


class CDFConfigTomlProvider(providers.TomlFileProvider):
    """An opinionated config provider for CDF."""

    def __init__(self, project_dir: str | Path = ".") -> None:
        super().__init__(
            c.CDF_CONFIG_FILE, project_dir=str(project_dir), add_global_config=True
        )

    name = c.CDF_CONFIG_FILE  # type: ignore[assignment]
    supports_secrets = False  # type: ignore[assignment]
    is_writable = True  # type: ignore[assignment]


class CDFSecretsTomlProvider(providers.TomlFileProvider):
    """An opinionated secrets provider for CDF."""

    def __init__(self, project_dir: str | Path = ".") -> None:
        super().__init__(
            c.CDF_SECRETS_FILE, project_dir=str(project_dir), add_global_config=True
        )

    name = c.CDF_SECRETS_FILE  # type: ignore[assignment]
    supports_secrets = True  # type: ignore[assignment]
    is_writable = True  # type: ignore[assignment]


def get_config_providers(
    search_paths: t.Sequence[str | Path] | str | Path = c.COMPONENT_PATHS,
    search_cwd: bool = True,
    max_depth: int = 3,
) -> t.List[providers.ConfigProvider]:
    """Get the first config provider found in the search paths.

    We search in the order of the search paths, and the first provider found is returned. We
    expect both cdf_config.toml and cdf_secrets.toml to be in the same directory wherever the
    first one is found. An empty list is returned if no config provider is found. The net effect
    of this approach is that the config provider found is the one closest to the current working
    directory but we are not constrained to be in the same directory as the config provider.

    Args:
        search_paths: The paths to search for config files.
        search_cwd: Whether to search the current working directory.
        max_depth: The maximum depth to search.

    Returns:
        The first config provider found.
    """
    if isinstance(search_paths, (str, Path)):
        search_paths = [search_paths]
    if search_cwd:
        t.cast(t.List[str], search_paths).insert(0, ".")
    providers = []
    for raw_path in search_paths:
        path, local_depth = Path(raw_path).expanduser().resolve(), 0
        while local_depth < max_depth and path != path.parent:
            if path.joinpath(c.CDF_CONFIG_FILE).exists():
                providers.append(CDFConfigTomlProvider(project_dir=path))
            if path.joinpath(c.CDF_SECRETS_FILE).exists():
                providers.append(CDFSecretsTomlProvider(project_dir=path))
            # Get the first instance or continue traversing?
            # Current decision is to get the first instance
            if providers:
                break
            path = path.parent
            local_depth += 1
        if providers:
            break
    return providers


def extend_global_providers(providers: t.List[providers.ConfigProvider]) -> int:
    """Extend the global config providers with the given providers.

    Args:
        providers: The providers to extend the global config providers with.

    Returns:
        The number of providers added. -1 if the global config providers were already extended.
    """
    with contextlib.suppress(dlt_e.DuplicateConfigProviderException):
        return len(do(Container()[ConfigProvidersContext].add_provider, providers))
    return -1
