"""CDF configuration

Config can be defined as a dictionary or a file path. If a file path is given, then it must be
either JSON, YAML or TOML format.
"""

import typing as t
from pathlib import Path

import dynaconf

import cdf.core.constants as c
from cdf.types import M, PathLike

SUPPORTED_EXTENSIONS = ["toml", "yaml", "yml", "json", "py"]


class ParsedConfiguration(t.TypedDict):
    """A container for configuration data"""

    root: Path
    project: dynaconf.Dynaconf
    workspaces: t.Dict[str, dynaconf.Dynaconf]


def _load(path: Path) -> dynaconf.LazySettings:
    """Load configuration data from a file path.

    Args:
        path: The path to the project or workspace directory

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
def load_config(root: PathLike) -> ParsedConfiguration:
    """Load configuration data from a project root path.

    Args:
        root: The root path to the project.

    Returns:
        A Result monad with the configuration data if successful. Otherwise, a Result monad with an
        error.
    """
    root_path = Path(root).resolve()
    if not root_path.is_dir():
        raise FileNotFoundError(f"Project not found: {root_path}")
    project = _load(root_path)
    assert project.name, "Project name must be defined"
    workspaces = {}
    workspace_paths = [
        root_path / workspace for workspace in project.setdefault("workspaces", ["."])
    ]
    for workspace in workspace_paths:
        if not workspace.is_dir():
            raise FileNotFoundError(f"Workspace not found: {workspace}")
        config = _load(workspace)
        workspaces[config.name] = config
    return ParsedConfiguration(root=root_path, project=project, workspaces=workspaces)
