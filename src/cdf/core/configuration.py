"""CDF configuration

Config can be defined as a dictionary or a file path. If a file path is given, then it must be
either JSON, YAML or TOML format.
"""

from pathlib import Path

import dynaconf

from cdf.types import M, PathLike

SUPPORTED_EXTENSIONS = ["toml", "yaml", "yml", "json"]


def load_project_config(
    root: PathLike,
) -> M.Result[dynaconf.Dynaconf, Exception]:
    """Load configuration data from a project root path.

    Args:
        root: The root path to the project.

    Returns:
        A Result monad with the configuration data if successful. Otherwise, a Result monad with an
        error.
    """
    try:
        project = Path(root).resolve()
        if not project.is_dir():
            return M.error(FileNotFoundError(f"Workspace not found: {project}"))
        settings = dynaconf.LazySettings(
            root_path=project,
            settings_files=[f"cdf_project.{ext}" for ext in SUPPORTED_EXTENSIONS],
            includes=[f".cdf_project.{ext}" for ext in SUPPORTED_EXTENSIONS],
            environments=True,
            envvar_prefix="CDF",
            env_switcher="CDF_ENVIRONMENT",
            load_dotenv=True,
            envvar="CDF_CONFIG_PATH",
            merge_enabled=True,
            validators=[dynaconf.Validator("name", must_exist=True)],
        )
    except Exception as e:
        return M.error(e)
    else:
        return M.ok(settings)
