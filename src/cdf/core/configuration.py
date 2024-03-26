"""CDF configuration

Config can be defined as a dictionary or a file path. If a file path is given, then it must be
either JSON, YAML or TOML format.
"""

import typing as t
from contextlib import contextmanager, suppress
from dataclasses import dataclass
from pathlib import Path

import dynaconf

import cdf.core.constants as c
import cdf.core.context as context
from cdf.types import M, PathLike

SUPPORTED_EXTENSIONS = ["toml", "yaml", "yml", "json", "py"]


@dataclass
class Configuration:
    """A container for configuration data providing a dictionary-like API to access the values."""

    root_path: Path
    project_name: str
    settings: dynaconf.Dynaconf
    workspace_settings: t.Dict[str, dynaconf.Dynaconf]

    def __getattr__(self, name: str) -> dynaconf.Dynaconf:
        """Get a workspace configuration by name.

        Args:
            name: The name of the workspace.

        Returns:
            The project configuration.
        """
        if name in self.workspace_settings:
            return self.workspace_settings[name]
        raise AttributeError(
            f"'{self.__class__.__name__}' object has no attribute '{name}'"
        )

    @contextmanager
    def scope(self, workspace: str) -> t.Iterator["Configuration"]:
        """Set the workspace for scoped configuration resolution

        Args:
            workspace: The workspace with which to resolve configuration

        Returns:
            The workspace configuration
        """
        try:
            token = context.active_workspace.set(workspace)
            yield self
        finally:
            context.active_workspace.reset(token)

    def __getitem__(self, key: t.Union[str, t.Tuple[str, str]]) -> t.Any:
        """Get a configuration value by key.

        Args:
            key: The key of the configuration value or a tuple of the workspace name and the key.

        Returns:
            The configuration value.
        """
        if isinstance(key, tuple):
            if (v := self.workspace_settings[key[0]].get(key[1])) is not None:
                return v
            return self.settings[key[1]]
        workspace_context = context.active_workspace.get()
        if workspace_context is None:
            return self.settings[key]
        with suppress(KeyError):
            return self.workspace_settings[workspace_context][key]
        return self.settings[key]

    def to_dict(self) -> t.Dict[str, t.Any]:
        """Dump the realized configuration data to a dictionary.

        Returns:
            A dictionary with the configuration data.
        """
        return {
            "root_path": self.root_path,
            "project_name": self.project_name,
            "settings": self.settings.to_dict(),
            "workspace_settings": {
                name: settings.to_dict()
                for name, settings in self.workspace_settings.items()
            },
        }

    @classmethod
    def from_dict(cls, data: t.Dict[str, t.Any]) -> "Configuration":
        """Load realized configuration data from a dictionary.

        Args:
            data: The configuration data.
        """

        def _to_dynaconf(_data: t.Dict[str, t.Any]) -> dynaconf.LazySettings:
            settings = dynaconf.LazySettings()
            settings.update(_data)
            settings._wrapped.validators.validate()
            return settings

        return cls(
            root_path=Path(data["root_path"]),
            project_name=data["project_name"],
            settings=_to_dynaconf(data["settings"]),
            workspace_settings={
                name: _to_dynaconf(settings)
                for name, settings in data["workspace_settings"].items()
            },
        )


def _load(path: Path) -> dynaconf.LazySettings:
    """Load configuration data from a file path.

    Args:
        path: The path to the configuration file.

    Returns:
        A dynaconf.LazySettings object.
    """
    if not any(
        map(lambda ext: path.joinpath(f"cdf.{ext}").is_file(), SUPPORTED_EXTENSIONS)
    ):
        raise FileNotFoundError(f"No cdf configuration file found: {path}")
    return dynaconf.LazySettings(
        root_path=path,
        settings_files=[f"cdf.{ext}" for ext in SUPPORTED_EXTENSIONS],
        environments=True,
        envvar_prefix="CDF",
        env_switcher=c.CDF_ENVIRONMENT,
        env=c.DEFAULT_ENVIRONMENT,
        load_dotenv=True,
        envvar=c.CDF_ROOT,
        merge_enabled=True,
        validators=[dynaconf.Validator("name", must_exist=True)],
    )


@M.result
def load_config(root: PathLike) -> Configuration:
    """Load configuration data from a project root path.

    Args:
        root: The root path to the project.

    Returns:
        A Result monad with the configuration data if successful. Otherwise, a Result monad with an
        error.
    """
    project = Path(root).resolve()
    if not project.is_dir():
        raise FileNotFoundError(f"Project not found: {project}")
    project_config = _load(project)
    assert project_config.name, "Project name must be defined"
    workspace_configs = {}
    workspace_paths = [
        project / workspace
        for workspace in project_config.setdefault("workspaces", ["."])
    ]
    for workspace in workspace_paths:
        if not workspace.is_dir():
            raise FileNotFoundError(f"Workspace not found: {workspace}")
        config = _load(workspace)
        workspace_configs[config.name] = config
    return Configuration(
        root_path=project,
        project_name=project_config.name,
        settings=project_config,
        workspace_settings=workspace_configs,
    )
