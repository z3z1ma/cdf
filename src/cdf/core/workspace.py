import os
import subprocess
import sys
import typing as t
from contextlib import contextmanager, suppress
from functools import lru_cache
from pathlib import Path
from threading import Lock
from types import MappingProxyType, ModuleType

import dlt
import dotenv
import sqlmesh
import tomlkit as toml
import virtualenv

import cdf.core.constants as c
import cdf.core.feature_flags as ff
import cdf.core.logger as logger
from cdf.core.publisher import publisher_spec
from cdf.core.sink import destination, gateway, sink_spec
from cdf.core.source import CDFSource, pipeline_spec
from cdf.core.transform import CDFTransformLoader
from cdf.core.utils import load_module_from_path

_IMPORT_LOCK = Lock()


class Project:
    """A project encapsulates a collection of workspaces."""

    def __init__(
        self, workspaces: t.List["Workspace"], root_name: str | None = None
    ) -> None:
        """Initialize a project.

        Args:
            workspaces (t.List[Workspace]): List of workspaces.
            root_name (str, optional): Name of root path in project. Defaults to the current working directory.
        """
        self._workspaces = {ws.namespace: ws for ws in workspaces}
        self.root_name = root_name or Path.cwd().name
        self.meta = {}

    @property
    def workspaces(self) -> MappingProxyType[str, "Workspace"]:
        return MappingProxyType(self._workspaces)

    def add_workspace(self, workspace: "Workspace", replace: bool = True) -> None:
        """Add a workspace to the project.

        Raises:
            ValueError if workspace already exists and replace is False.

        Args:
            workspace (Workspace): The workspace to add.
        """
        if workspace.namespace in self._workspaces and not replace:
            raise ValueError(
                f"Workspace with namespace {workspace.namespace} already exists"
            )
        self._workspaces[workspace.namespace] = workspace

    def remove_workspace(self, namespace: str) -> "Workspace":
        """Remove a workspace from the project.

        Raises:
            KeyError if workspace does not exist.

        Args:
            namespace (str): The namespace of the workspace to remove.
        """
        return self._workspaces.pop(namespace)

    def __getattr__(self, name: str) -> "Workspace":
        try:
            return self._workspaces[name]
        except KeyError:
            raise AttributeError(f"Project {self.root_name} has no workspace {name}")

    def __getitem__(self, name: str) -> "Workspace":
        return self._workspaces[name]

    def __iter__(self) -> t.Iterator[t.Tuple[str, "Workspace"]]:
        return iter(self._workspaces.items())

    def __len__(self) -> int:
        return len(self._workspaces)

    def __contains__(self, name: str) -> bool:
        return name in self._workspaces

    def __repr__(self) -> str:
        ws = ", ".join(f"'{ns}'" for ns in self._workspaces.keys())
        return f"Project({self.root_name}, workspaces=[{ws}])"

    def __add__(self, other: "Project | Workspace") -> "Project":
        if isinstance(other, Workspace):
            self.add_workspace(other)
            return self
        elif isinstance(other, Project):
            proj = Project(
                list(self._workspaces.values()) + list(other._workspaces.values()),
                self.root_name,
            )
            proj.meta = {**self.meta, **other.meta}
            return proj
        else:
            return NotImplemented

    def keys(self) -> t.Set[str]:
        return set(self._workspaces.keys())

    def get_transform_context(
        self, workspaces: t.Sequence[str], sink: str | None = None
    ) -> sqlmesh.Context:
        """Get a sqlmesh context for a list of workspaces.

        Args:
            workspaces (t.Tuple[str, ...]): List of workspace namespaces.
            sink (str, optional): Name of transform gateway. Defaults to None.

        Returns:
            sqlmesh.Context: A sqlmesh context.
        """
        configs = {}
        for ws in workspaces:
            transform_opts = {}
            with self[ws].overlay(), suppress(KeyError):
                transform_opts = dlt.config["transforms"]
            configs[self[ws].root] = (
                self[ws].sinks[sink or "default"].transform_config(**transform_opts)
            )
        return sqlmesh.Context(
            config=configs,
            paths=[str(self[ws].root) for ws in workspaces],
            gateway="cdf_managed",
            loader=CDFTransformLoader,
        )

    @classmethod
    def from_dict(cls, workspaces: t.Dict[str, Path | str]) -> "Project":
        """Create a project from a dictionary of paths.

        Args:
            members (t.Dict[str, Path | str]): Dictionary of members.
        """
        return cls([Workspace(path, ns) for ns, path in workspaces.items()])

    @classmethod
    def default(
        cls,
        path: Path | str | None = None,
        load_dotenv: bool = True,
        append_syspath: bool = True,
    ) -> "Project":
        """Create a project from the current working directory."""
        if path is None:
            path = Path.cwd()
        elif isinstance(path, str):
            path = Path(path).expanduser().resolve()
        if load_dotenv:
            dotenv.load_dotenv(path / ".env")
        if append_syspath:
            sys.path.append(str(path))
        return cls([Workspace.find_nearest(path, raise_no_marker=True)])

    @classmethod
    def from_workspace_toml(
        cls, path: Path | str, load_dotenv: bool = True, append_syspath: bool = True
    ) -> "Project":
        """Create a project from a workspace.toml file.

        This is the canonical way to create a project. The workspace.toml file is a TOML file
        that contains a [workspace] section with a members key. The members key is a list of
        member specs. For example:

        [workspace]
        members = [
            "projects/data",
            "projects/marketing"
        ]

        Args:
            path (Path): Path to workspace.toml.
        """
        if isinstance(path, str):
            path = Path(path).expanduser().resolve()

        if not path.exists():
            raise ValueError(f"Could not find workspace.toml at {path}")

        with open(path) as f:
            conf = toml.load(f).get("workspace", {"members": []})

        parsed = {}
        for spec in conf["members"]:
            if ":" in spec:
                namespace, subpath = spec.split(":", 1)
            else:
                subpath = spec
                namespace = Path(subpath).name
            parsed[namespace] = path.parent / subpath

        if load_dotenv:
            dotenv.load_dotenv(path.parent / ".env")
        if append_syspath:
            sys.path.append(str(path.parent))

        return cls.from_dict(parsed)

    @classmethod
    def find_nearest(
        cls,
        path: Path | str | None = None,
        raise_no_marker: bool = False,
        load_dotenv: bool = True,
        append_syspath: bool = True,
    ) -> "Project":
        """Find nearest project.

        If no cdf_workspace.toml file is found, returns the current working directory as a project.

        Args:
            path (Path): The path to search from.
            raise_no_marker (bool, optional): Whether to raise an error if no project is found.
            load_dotenv (bool, optional): Whether to load the .env file if no project is found.
            append_syspath (bool, optional): Whether to append the project root to sys.path.
        """
        if path is None:
            path = Path.cwd()
        elif isinstance(path, str):
            path = Path(path).expanduser().resolve()
        orig_path = path
        while path.parents:
            workspace_spec = path / c.WORKSPACE_FILE
            if workspace_spec.exists():
                return cls.from_workspace_toml(
                    workspace_spec,
                    load_dotenv=load_dotenv,
                    append_syspath=append_syspath,
                )
            path = path.parent
        if raise_no_marker:
            raise ValueError(
                f"Could not find a project root in {path} or any of its parents"
            )
        return cls.default(
            orig_path, load_dotenv=load_dotenv, append_syspath=append_syspath
        )


class WorkspaceCapabilities(t.TypedDict):
    """A dict which describes the capabilties available within a workspace"""

    pipeline: bool
    publish: bool
    transform: bool
    dependency: bool


T = t.TypeVar("T")
P = t.ParamSpec("P")


def requires_pipelines(func: t.Callable[P, T]) -> t.Callable[P, T]:
    """Decorator to ensure that a workspace has pipelines.

    Raises:
        ValueError if workspace has no pipelines.
    """

    def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
        self = t.cast("Workspace", args[0])
        if not self.has_pipelines:
            raise ValueError(f"Workspace {self.root} has no pipelines")
        return func(*args, **kwargs)

    return wrapper


def requires_publishers(func: t.Callable[P, T]) -> t.Callable[P, T]:
    """Decorator to ensure that a workspace has publishers.

    Raises:
        ValueError if workspace has no publishers.
    """

    def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
        self = t.cast("Workspace", args[0])
        if not self.has_publishers:
            raise ValueError(f"Workspace {self.root} has no publishers")
        return func(*args, **kwargs)

    return wrapper


def requires_transforms(func: t.Callable[P, T]) -> t.Callable[P, T]:
    """Decorator to ensure that a workspace has transforms.

    Raises:
        ValueError if workspace has no transforms.
    """

    def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
        self = t.cast("Workspace", args[0])
        if not self.has_transforms:
            raise ValueError(f"Workspace {self.root} has no transforms")
        return func(*args, **kwargs)

    return wrapper


def requires_dependencies(func: t.Callable[P, T]) -> t.Callable[P, T]:
    """Decorator to ensure that a workspace has dependencies.

    Raises:
        ValueError if workspace has no dependencies.
    """

    def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
        self = t.cast("Workspace", args[0])
        if not self.has_dependencies:
            raise ValueError(f"Workspace {self.root} has no dependencies")
        return func(*args, **kwargs)

    return wrapper


class Workspace:
    """A workspace encapsulates a directory containing pipelines, publishers, metadata, and transforms.

    We can think of a Workspace as a pathlib.Path with some additional functionality. A Workspace
    has capabilities based on the presence of certain directories. For example, if a workspace has
    a `pipelines` directory, we can say that the workspace has the capability to ingest data. If a
    workspace has a `publishers` directory, we can say that the workspace has the capability to
    publish data. If a workspace has a `transforms` directory, we can say that the workspace has
    the capability to transform data. A workspace may have any combination of these capabilities.

    Each of these capabilties is exposed as a property on the Workspace class.
    """

    ROOT_MARKERS = [
        ".git",
        c.CONFIG_FILE,
        c.SECRETS_FILE,
        c.PIPELINES_PATH,
        c.TRANSFORMS_PATH,
        c.PUBLISHERS_PATH,
    ]

    def __init__(
        self,
        root: str | Path,
        namespace: str = c.DEFAULT_WORKSPACE,
    ) -> None:
        """Initialize a workspace.

        Args:
            root (str | Path): Path to wrap as a workspace.
            namespace (str, optional): Namespace of workspace. Defaults to c.DEFAULT_WORKSPACE.
        """
        self.meta = {}
        self.namespace = namespace
        self._root = Path(root).expanduser().resolve()
        if not self._root.exists():
            raise ValueError(
                f"Tried to init Workspace with nonexistent path {self.root}"
            )

        self.pipeline_paths = self._get_python_fpaths(c.PIPELINES_PATH)
        self.publisher_paths = self._get_python_fpaths(c.PUBLISHERS_PATH)
        self.script_paths = self._get_python_fpaths(c.SCRIPTS_PATH)
        self.transform_path = self.root / c.TRANSFORMS_PATH
        self.config_path = self.root / c.CONFIG_FILE
        self.secrets_path = self.root / c.SECRETS_FILE
        self.lockfile_path = self.root / c.LOCKFILE_PATH
        self.sinks_path = self.root / c.SINKS_FILE
        self.requirements_path = self.root / c.REQUIREMENTS_FILE

        self._overlay_active = False
        self._did_inject_config_providers = False

        self._pipelines = {}
        self._publishers = {}
        self._sinks = {}
        self._scripts = {}

        if not self.lockfile_path.exists():
            self.lockfile_path.touch()

    @property
    def root(self) -> Path:
        """Root path of workspace."""
        return self._root

    @property
    def has_pipelines(self) -> bool:
        """True if workspace has pipelines."""
        return len(self.pipeline_paths) > 0

    @property
    def has_publishers(self) -> bool:
        """True if workspace has publishers."""
        return len(self.publisher_paths) > 0

    @property
    def has_transforms(self) -> bool:
        """True if workspace has transforms."""
        rv = self.transform_path.exists() and list(
            path
            for ext in ["sql", "yml", "yaml"]
            for path in self.transform_path.rglob(f"*.{ext}")
            if path.is_file()
        )
        return bool(rv)

    @property
    def has_dependencies(self) -> bool:
        """True if workspace has a virtual environment spec."""
        return (
            self.requirements_path.exists()
            and len(self.requirements_path.read_text().strip().splitlines()) > 0
        )

    @property
    def capabilities(self) -> WorkspaceCapabilities:
        """Get the capabilities for the workspace"""
        return {
            "pipeline": self.has_pipelines,
            "transform": self.has_transforms,
            "publish": self.has_publishers,
            "dependency": self.has_dependencies,
        }

    @property
    def python_path(self) -> Path:
        """Get path to Workspace python.

        Returns:
            Path to python executable. If there is no requirements.txt, returns system python
        """
        if not self.has_dependencies:
            return Path(sys.executable)
        return self.root / c.VENV_PATH / "bin" / "python"

    @property
    def pip_path(self) -> Path:
        """Get path to workspace pip.

        Returns:
            Path to pip executable. If there is no requirements.txt, returns the most probable system pip
        """
        if not self.has_dependencies:
            return Path(sys.executable).parent / "pip"
        return self.root / c.VENV_PATH / "bin" / "pip"

    @requires_dependencies
    def get_bin(self, name: str, must_exist: bool = False) -> Path:
        """Get path to binary in workspace.

        Args:
            name (str): Name of binary.
            must_exist (bool): If True, raises ValueError if binary does not exist.

        Raises:
            ValueError if binary does not exist and must_exist is True or if workspace has no
                dependencies.

        Returns:
            Path to binary.
        """
        bin_path = self.root / c.VENV_PATH / "bin" / name
        if must_exist and not bin_path.exists():
            raise ValueError(f"Could not find bin {name} in {self.root}")
        return bin_path

    @lru_cache(maxsize=1)
    def read_lock(self) -> dict:
        """Read lockfile.

        Returns:
            Dictionary of lockfile contents.
        """
        with suppress(FileNotFoundError):
            return toml.loads(self.lockfile_path.read_text())
        self.lockfile_path.touch()
        return {}

    def write_lock(self, lockfile: dict) -> int:
        """Write lockfile.

        Args:
            lockfile (dict): Dictionary of lockfile contents.

        Returns:
            Number of bytes written.
        """
        self.read_lock.cache_clear()
        return self.lockfile_path.write_text(toml.dumps(lockfile))

    def put_kv_lock(self, key: str, value: t.Any) -> int:
        """Put a value in the lockfile. Overwrites existing values.

        Args:
            key (str): Key to put.
            value (t.Any): Value to put.

        Returns:
            Number of bytes written.
        """
        lockfile = self.read_lock()
        lockfile[key] = value
        return self.write_lock(lockfile)

    def get_kv_lock(self, key: str) -> t.Dict[str, t.Any]:
        """Get a value from the lockfile.

        Args:
            key (str): Key to get.

        Returns:
            Value from lockfile.
        """
        return self.read_lock()[key]

    @requires_dependencies
    def _setup_deps(self, force: bool = False) -> None:
        """Install dependencies if requirements.txt is newer than virtual environment."""
        req_mtime = self.requirements_path.stat().st_mtime
        venv_mtime = (self.root / c.VENV_PATH).stat().st_mtime
        if (req_mtime > venv_mtime) or force:
            logger.info("Change detected. Updating dependencies for %s", self.root)
            subprocess.check_call(
                [
                    self.pip_path,
                    "install",
                    "--upgrade",
                    "-r",
                    self.requirements_path,
                ]
            )
            (self.root / c.VENV_PATH).touch()

    @requires_dependencies
    def _setup_venv(self) -> None:
        """Create a virtual environment.

        The canonical route to this method is via ensure_venv, but developers can call this
        directly for more control or override the implementation in a Workspace subclass.
        """
        virtualenv.cli_run(
            [
                str(self.root / c.VENV_PATH),
                "--symlink-app-data",
                "--download",
                "--pip=bundle",
                "--setuptools=bundle",
                "--wheel=bundle",
                "--system-site-packages",
                "--prompt",
                f"cdf.{self.namespace}",
            ]
        )
        self.requirements_path.touch()

    def ensure_venv(self) -> None:
        """Create a virtual environment for the workspace if it does not exist.

        This method creates a virtual environment for the workspace if it does not exist. It also
        installs the requirements.txt into the virtual environment. If the requirements.txt is
        newer than the virtual environment, it reinstalls the requirements.txt. If the workspace has no
        dependencies, this method is a no-op.
        """
        if self.has_dependencies:
            if not self.python_path.exists():
                self._setup_venv()
            self._setup_deps(force=False)

    _mod_cache: t.Dict[str, ModuleType] = {}
    """Class var to cache modules between runs of the context manager"""

    @contextmanager
    def overlay(self) -> t.Iterator[None]:
        """Context manager to fully configure a workspace for runtime use.

        This context manager ensures that side effects from importing modules in the workspace
        or from activating the virtual environment are contained to the context. It does this by
        storing the original sys.path, sys.prefix, sys.modules, and os.environ and restoring them
        after the context exits. It caches the modules that were imported during the context and
        restores them on re-entry to ensure consistent interpreter state. This is not as idiomatic
        as a subprocess, but is significantly faster and allows us to use the same interpreter. It
        also injects workspace config into the context.
        """
        if self._overlay_active:
            yield
            return
        self.ensure_venv()
        activate = self.root / c.VENV_PATH / "bin" / "activate_this.py"
        environ, syspath, sysprefix, sysmodules = (
            os.environ.copy(),
            sys.path.copy(),
            sys.prefix,
            sys.modules.copy(),
        )
        if self.has_dependencies:
            exec(activate.read_bytes(), {"__file__": str(activate)})
        sys.path.append(str(self.root))
        if self._mod_cache:
            sys.modules.update(self._mod_cache)
        dotenv.load_dotenv(self.root / ".env")
        with self.providers():
            self._overlay_active = True
            yield
        self._mod_cache = sys.modules.copy()
        new_modules = set(sys.modules) - set(sysmodules)
        for mod in new_modules:
            # Make an effort to scrub modules that were imported from the workspace
            p = getattr(sys.modules[mod], "__file__", None)
            if p is not None and Path(p).is_relative_to(self.root):
                del sys.modules[mod]
        os.environ, sys.path, sys.prefix = (
            environ,
            syspath,
            sysprefix,
        )
        self._overlay_active = False

    def inject_workspace_config_providers(self) -> None:
        """Inject workspace config into context"""
        from cdf.core.config import inject_config_providers_from_workspace

        if self._did_inject_config_providers:
            return

        inject_config_providers_from_workspace(workspace=self)
        self._did_inject_config_providers = True

    @classmethod
    def find_nearest(
        cls, path: Path | str | None = None, raise_no_marker: bool = False
    ) -> "Workspace":
        if path is None:
            path = Path.cwd()
        elif isinstance(path, str):
            path = Path(path).expanduser().resolve()

        while path.parents:
            if any((path / marker).exists() for marker in cls.ROOT_MARKERS):
                return cls(path)
            path = path.parent
        if raise_no_marker:
            raise ValueError(
                f"Could not find a workspace root in {path} or any of its parents"
            )
        return cls(path)

    @property
    @requires_pipelines
    def pipelines(self) -> t.Dict[str, pipeline_spec]:
        """Load pipelines from workspace."""
        if not self._pipelines:
            with (
                _IMPORT_LOCK,
                self.overlay(),
            ):
                for path in self.pipeline_paths:
                    mod, _ = load_module_from_path(path)
                    for spec in getattr(mod, c.CDF_PIPELINES, []):
                        if isinstance(spec, dict):
                            spec = pipeline_spec(**spec)
                        assert isinstance(
                            spec, pipeline_spec
                        ), f"{spec} is not a pipeline"
                        self._pipelines[spec.name] = spec
        return self._pipelines

    @property
    @requires_publishers
    def publishers(self) -> t.Dict[str, publisher_spec]:
        """Load publishers from workspace."""
        if not self._publishers:
            with (
                _IMPORT_LOCK,
                self.overlay(),
            ):
                for path in self.publisher_paths:
                    mod, _ = load_module_from_path(path)
                    for spec in getattr(mod, c.CDF_PUBLISHERS, []):
                        if isinstance(spec, dict):
                            spec = publisher_spec(**spec)
                        assert isinstance(
                            spec, publisher_spec
                        ), f"{spec} is not a publisher"
                        self._publishers[spec.name] = spec
        return self._publishers

    @property
    def sinks(self) -> t.Dict[str, sink_spec]:
        """Load publishers from workspace."""
        if not self._sinks:
            with (
                _IMPORT_LOCK,
                self.overlay(),
            ):
                mod, _ = load_module_from_path(self.root / "cdf_sinks.py")
                for spec in getattr(mod, c.CDF_SINKS, []):
                    if isinstance(spec, dict):
                        spec = sink_spec(**spec)
                    assert isinstance(spec, sink_spec), f"{spec} is not a sink"
                    self._sinks[spec.name] = spec
            self._sinks.setdefault(
                "default",
                sink_spec(
                    name="default",
                    environment="dev",
                    destination=destination.duckdb(
                        "cdf.duckdb", destination_name="default"
                    ),
                    gateway=gateway.parse_obj(
                        {"connection": {"type": "duckdb", "database": "cdf.duckdb"}}
                    ),
                ),
            )
        return self._sinks

    @t.runtime_checkable
    class Script(t.Protocol):
        def __call__(self, workspace: "Workspace", **kwargs: t.Any) -> None:
            ...

    @property
    def scripts(self) -> t.Dict[str, Script]:
        """Load scripts from workspace."""
        if not self._scripts:
            with (
                _IMPORT_LOCK,
                self.overlay(),
            ):
                for path in self.script_paths:
                    mod, _ = load_module_from_path(path)
                    entrypoint = getattr(mod, "entrypoint", None)
                    if entrypoint is None:
                        raise ValueError(
                            f"Could not find entrypoint in {path} for script {mod}"
                        )
                    if not isinstance(entrypoint, Workspace.Script):
                        raise ValueError(
                            f"Entrypoint in {path} for script {mod} is not a Script."
                            " Scripts must be callables which take a Workspace as the first argument."
                        )
                    self._scripts[mod.__name__] = entrypoint
        return self._scripts

    @property
    @requires_transforms
    def transforms(self) -> t.Mapping[str, sqlmesh.Model]:
        """Load transforms from workspace using the first available context."""
        context = self.transform_context()
        return context.models

    @lru_cache(maxsize=1)
    def transform_context(self, sink: str | None = None) -> sqlmesh.Context:
        """Get a sqlmesh context for the workspace.

        This method loads the sqlmesh config from the workspace config file and returns a
        sqlmesh context.

        Args:
            sink (str, optional): Name of transform gateway. Defaults to None.

        Returns:
            sqlmesh.Context: A sqlmesh context.
        """
        transform_opts = {}
        with self.overlay(), suppress(KeyError):
            transform_opts = dlt.config["transforms"]
        if sink is None:
            sink_ = next(s for s in self.sinks.values() if s.gateway)
        else:
            sink_ = self.sinks[sink]
        return sink_.transform_context(str(self.root), **transform_opts)

    @contextmanager
    @requires_pipelines
    def runtime_source(self, pipeline_name: str, **kwargs) -> t.Iterator[CDFSource]:
        """Get a runtime source from the workspace.

        A runtime source is a the equivalent to the source cdf would generate itself in a typical
        pipeline execution. The source is pulled out from the pipeline generator.

        Args:
            pipeline_name (str): Name of source to get.
            **kwargs: Keyword args to pass to pipeline function if it is a callable.
        """
        with self.overlay():
            ctx = self.pipelines[pipeline_name].unwrap(**kwargs)
            source = next(ctx)
            yield ff.process_source(source, ff.get_provider(self))

    @contextmanager
    def providers(self) -> t.Iterator[None]:
        """Context manager to inject workspace config into context."""
        from cdf.core.config import with_config_providers_from_workspace

        with with_config_providers_from_workspace(workspace=self):
            yield

    def _get_python_fpaths(
        self, *subpath: str, include_init: bool = False
    ) -> t.List[Path]:
        """List of paths to pipeline modules.

        Returns:
            List of paths to pipeline modules.
        """
        return [
            path
            for path in self.root.joinpath(*subpath).glob("*.py")
            if path.name != "__init__.py" or include_init
        ]

    def __repr__(self) -> str:
        return f"Workspace(root='{self._root.relative_to(Path.cwd())}', capabilities={self.capabilities})"

    def __add__(self, other: "Workspace") -> Project:
        return Project([self, other])

    def __radd__(self, other: "Workspace") -> Project:
        return Project([other, self])

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Workspace):
            return NotImplemented
        return self.root == other.root

    def __hash__(self) -> int:
        return hash(self.root)
