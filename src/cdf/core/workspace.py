import subprocess
import sys
import typing as t
import venv
from pathlib import Path

import tomlkit as toml

import cdf.core.constants as c


class Project:
    """A project encapsulates a collection of workspaces."""

    def __init__(self, members: t.Dict[str, "Workspace"]) -> None:
        self._members = members
        self.options = {}

    @property
    def members(self) -> t.Dict[str, "Workspace"]:
        return self._members

    def __getattr__(self, name: str) -> "Workspace":
        return self._members[name]

    def __getitem__(self, name: str) -> "Workspace":
        return self._members[name]

    def __iter__(self) -> t.Iterator["Workspace"]:
        return iter(self._members.values())

    def __len__(self) -> int:
        return len(self._members)

    def __contains__(self, name: str) -> bool:
        return name in self._members

    def __repr__(self) -> str:
        return f"Project({', '.join(self._members.keys())})"

    @classmethod
    def from_dict(cls, members: t.Dict[str, Path | str]) -> "Project":
        """Create a project from a dictionary of members.

        Args:
            members (t.Dict[str, Path | str]): Dictionary of members.
        """
        return cls({name: Workspace(path) for name, path in members.items()})

    @classmethod
    def from_workspace_toml(cls, path: Path | str) -> "Project":
        """Create a project from a workspace.toml file.

        This is the canonical way to create a project. The workspace.toml file is a TOML file
        that contains a [workspace] section with a members key. The members key is a list of
        member specs. For example:

        [workspace]
        members = [
            "data:projects/data",
            "marketing:projects/marketing"
        ]

        Args:
            path (Path): Path to workspace.toml.
        """
        if isinstance(path, str):
            path = Path(path).expanduser().resolve()

        with open(path) as f:
            conf = toml.load(f).get("workspace", {"members": []})

        parsed = {}
        for spec in conf["members"]:
            namespace, subpath = spec.split(":", 1)
            parsed[namespace] = path.parent / subpath
        return cls.from_dict(parsed)

    @classmethod
    def find_nearest(cls, path: Path | str | None = None) -> "Project":
        """Find nearest project.

        Args:
            path (Path): The path to search from.
        """
        if path is None:
            path = Path.cwd()
        elif isinstance(path, str):
            path = Path(path).expanduser().resolve()
        while path.parents:
            workspace_spec = path / c.CDF_WORKSPACE_FILE
            if workspace_spec.exists():
                return cls.from_workspace_toml(workspace_spec)
            path = path.parent
        raise ValueError("No workspace found.")


class WorkspaceCapabilities(t.TypedDict):
    """A dict which describes the capabilties available within a workspace"""

    ingest: bool
    publish: bool
    transform: bool
    managed_venv: bool


class Workspace:
    """A workspace encapsulates a directory containing sources, publishers, metadata, and transforms.

    We can think of a Workspace as a pathlib.Path with some additional functionality. A Workspace
    has capabilities based on the presence of certain directories. For example, if a workspace has
    a `sources` directory, we can say that the workspace has the capability to ingest data. If a
    workspace has a `publishers` directory, we can say that the workspace has the capability to
    publish data. If a workspace has a `transforms` directory, we can say that the workspace has
    the capability to transform data. A workspace may have any combination of these capabilities.

    Each of these capabilties is exposed as a property on the Workspace class.
    """

    ROOT_MARKERS = [
        ".git",
        "cdf_config.toml",
        "cdf_secrets.toml",
    ]

    def __init__(self, root: str | Path) -> None:
        """Initialize a workspace.

        Args:
            root (str | Path): Path to wrap as a workspace.
        """
        self._root = Path(root).expanduser().resolve()
        if not self._root.exists():
            raise ValueError(
                f"Tried to init Workspace with nonexistent path {self.root}"
            )
        self._source_paths = None
        self._publisher_paths = None
        self._requirements = None
        self._did_inject_config_providers = False

    @property
    def root(self) -> Path:
        """Root path of workspace."""
        return self._root

    @property
    def has_ingest_capability(self) -> bool:
        """True if workspace has sources."""
        return len(self.source_paths) > 0

    @property
    def has_publish_capability(self) -> bool:
        """True if workspace has publishers."""
        return len(self.publisher_paths) > 0

    @property
    def has_transform_capability(self) -> bool:
        """True if workspace has transforms."""
        return self.transform_path.exists()

    @property
    def has_managed_venv_capability(self) -> bool:
        """True if workspace has a virtual environment spec."""
        return self.requirements_path.exists()

    @property
    def capabilities(self) -> WorkspaceCapabilities:
        """Get the capabilities for the workspace"""
        return {
            "ingest": self.has_ingest_capability,
            "transform": self.has_transform_capability,
            "publish": self.has_publish_capability,
            "managed_venv": self.has_managed_venv_capability,
        }

    @property
    def source_paths(self) -> t.List[Path]:
        """List of paths to source modules."""
        if self._source_paths is None:
            self._source_paths = self._get_source_paths()
        return self._source_paths

    @property
    def publisher_paths(self) -> t.List[Path]:
        """List of paths to publisher modules."""
        if self._publisher_paths is None:
            self._publisher_paths = self._get_publisher_paths()
        return self._publisher_paths

    @property
    def transform_path(self) -> Path:
        """Path to transform directory."""
        return self.root / c.TRANSFORMS_PATH

    @property
    def config_path(self) -> Path:
        """Path to Workspace config file."""
        return self.root / c.CDF_CONFIG_FILE

    @property
    def secrets_path(self) -> Path:
        """Path to Workspace secrets file."""
        return self.root / c.CDF_SECRETS_FILE

    @property
    def requirements_path(self) -> Path:
        """Path to requirements.txt."""
        return self.root / "requirements.txt"

    @property
    def python_path(self) -> Path:
        """Get path to Workspace python.

        Returns:
            Path to python executable. If there is no requirements.txt, returns system python
        """
        if not self.has_managed_venv_capability:
            return Path(sys.executable)
        return self.root / ".venv" / "bin" / "python"

    @property
    def pip_path(self) -> Path:
        """Get path to workspace pip.

        Returns:
            Path to pip executable. If there is no requirements.txt, returns the most probable system pip
        """
        if not self.has_managed_venv_capability:
            return Path(sys.executable).parent / "pip"
        return self.root / ".venv" / "bin" / "pip"

    def _get_source_paths(self) -> t.List[Path]:
        """List of paths to source modules.

        Returns:
            List of paths to source modules.
        """
        return [
            path
            for path in self.root.joinpath(
                c.SOURCES_PATH,
            ).glob("*.py")
        ]

    def _get_publisher_paths(self) -> t.List[Path]:
        """List of paths to publisher modules.

        Returns:
            List of paths to publisher modules.
        """
        return [
            path
            for path in self.root.joinpath(
                c.PUBLISHERS_PATH,
            ).glob("*.py")
        ]

    def __repr__(self) -> str:
        return f"Workspace(root={self._root})"

    def ensure_venv(self) -> None:
        """Create a virtual environment for the workspace if it does not exist

        Uses the default venv.EnvBuilder class. Use setup_venv for more control over the behavior.
        """
        if self.has_managed_venv_capability and not self.python_path.exists():
            return self.setup_venv()

    def setup_venv(
        self, *, klass: t.Type[venv.EnvBuilder] = venv.EnvBuilder, **kwargs: t.Any
    ) -> None:
        """Create a virtual environment. Clear and reinstantiate env if it exists.

        The canonical route to this method is via ensure_venv, but developers can call this
        directly for more control or override the implementation in a Workspace subclass.

        Args:
            klass (t.Type[WorkspaceVenvCreator]): Class to use to create virtual environment.
            kwargs: Keyword arguments to pass to the venv builder create method.

        Raises:
            SubprocessError if pip fails to install the requirements.txt
        """
        klass(
            with_pip=True,
            symlinks=True,
            clear=True,
        ).create(self.root / ".venv", **kwargs)
        subprocess.check_call([self.pip_path, "install", "-r", self.requirements_path])

    def inject_workspace_config_providers(self, as_: str, /) -> None:
        """Inject workspace config into context

        Args:
            as_ (str): The name to inject the workspace as.
        """
        from cdf.core.config import config_provider_factory, inject_config_providers

        if self._did_inject_config_providers:
            return
        inject_config_providers(
            [
                config_provider_factory(
                    f"{as_}.config", project_dir=self.root, secrets=False
                ),
                config_provider_factory(
                    f"{as_}.secrets", project_dir=self.root, secrets=True
                ),
            ]
        )
        self._did_inject_config_providers = True

    @classmethod
    def find_nearest(cls, path: Path | str | None = None) -> "Workspace":
        if path is None:
            path = Path.cwd()
        elif isinstance(path, str):
            path = Path(path).expanduser().resolve()

        while path.parents:
            if any((path / marker).exists() for marker in cls.ROOT_MARKERS):
                return cls(path)
            path = path.parent
        raise ValueError("No workspace found.")
