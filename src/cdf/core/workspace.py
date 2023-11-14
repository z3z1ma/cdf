import os
import subprocess
import sys
import typing as t
from contextlib import contextmanager
from pathlib import Path
from threading import Lock
from types import MappingProxyType

import dotenv
import tomlkit as toml
import virtualenv
from dlt.common.pipeline import LoadInfo
from dlt.pipeline import Pipeline

import cdf.core.constants as c
from cdf.core.source import CDFSource, CDFSourceMeta
from cdf.core.utils import augmented_path

_IMPORT_LOCK = Lock()


class Project:
    """A project encapsulates a collection of workspaces."""

    def __init__(self, workspaces: t.List["Workspace"]) -> None:
        self._workspaces = {ws.namespace: ws for ws in workspaces}
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
                "Workspace with namespace %s already exists", workspace.namespace
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
        return self._workspaces[name]

    def __getitem__(self, name: str) -> "Workspace":
        return self._workspaces[name]

    def __iter__(self) -> t.Iterator[t.Tuple[str, "Workspace"]]:
        return iter(self._workspaces.items())

    def __len__(self) -> int:
        return len(self._workspaces)

    def __contains__(self, name: str) -> bool:
        return name in self._workspaces

    def __repr__(self) -> str:
        return f"Project({', '.join(self._workspaces.keys())})"

    @classmethod
    def from_dict(cls, workspaces: t.Dict[str, Path | str]) -> "Project":
        """Create a project from a dictionary of paths.

        Args:
            members (t.Dict[str, Path | str]): Dictionary of members.
        """
        return cls([Workspace(path, ns) for ns, path in workspaces.items()])

    @classmethod
    def default(cls, path: Path | str | None = None) -> "Project":
        """Create a project from the current working directory."""
        return cls([Workspace.find_nearest(path, raise_no_marker=True)])

    @classmethod
    def from_workspace_toml(
        cls, path: Path | str, load_dotenv: bool = True
    ) -> "Project":
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

        if not path.exists():
            raise ValueError(f"Could not find workspace.toml at {path}")

        with open(path) as f:
            conf = toml.load(f).get("workspace", {"members": []})

        parsed = {}
        for spec in conf["members"]:
            namespace, subpath = spec.split(":", 1)
            parsed[namespace] = path.parent / subpath

        if load_dotenv:
            dotenv.load_dotenv(path.parent / ".env")

        return cls.from_dict(parsed)

    @classmethod
    def find_nearest(
        cls, path: Path | str | None = None, raise_no_marker: bool = False
    ) -> "Project":
        """Find nearest project.

        If no cdf_workspace.toml file is found, returns the current working directory as a project.

        Args:
            path (Path): The path to search from.
        """
        if path is None:
            path = Path.cwd()
        elif isinstance(path, str):
            path = Path(path).expanduser().resolve()
        orig_path = path
        while path.parents:
            workspace_spec = path / c.CDF_WORKSPACE_FILE
            if workspace_spec.exists():
                return cls.from_workspace_toml(workspace_spec)
            path = path.parent
        if raise_no_marker:
            raise ValueError(
                f"Could not find a project root in {path} or any of its parents"
            )
        return cls.default(orig_path)


class WorkspaceCapabilities(t.TypedDict):
    """A dict which describes the capabilties available within a workspace"""

    ingest: bool
    publish: bool
    transform: bool
    deps: bool


class WorkspaceSourceContext(t.TypedDict):
    spec: CDFSourceMeta
    globals: dict
    locals: dict
    execution_ctx: t.Tuple[dict, dict]


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
        c.CDF_CONFIG_FILE,
        c.CDF_SECRETS_FILE,
        c.SOURCES_PATH,
        c.TRANSFORMS_PATH,
        c.PUBLISHERS_PATH,
    ]

    def __init__(
        self,
        root: str | Path,
        namespace: str = c.DEFAULT_WORKSPACE,
        load_dotenv: bool = True,
    ) -> None:
        """Initialize a workspace.

        Args:
            root (str | Path): Path to wrap as a workspace.
            load_dotenv (bool): Whether to load dotenv file in workspace root.
        """
        self.namespace = namespace
        self._root = Path(root).expanduser().resolve()
        if not self._root.exists():
            raise ValueError(
                f"Tried to init Workspace with nonexistent path {self.root}"
            )
        self._source_paths = None
        self._publisher_paths = None
        self._requirements = None
        self._did_inject_config_providers = False
        self._cached_sources = {}
        if load_dotenv:
            dotenv.load_dotenv(self.root / ".env")

    @property
    def root(self) -> Path:
        """Root path of workspace."""
        return self._root

    @property
    def has_sources(self) -> bool:
        """True if workspace has sources."""
        return len(self.source_paths) > 0

    @property
    def has_publishers(self) -> bool:
        """True if workspace has publishers."""
        return len(self.publisher_paths) > 0

    @property
    def has_transforms(self) -> bool:
        """True if workspace has transforms."""
        return self.transform_path.exists()

    @property
    def has_dependencies(self) -> bool:
        """True if workspace has a virtual environment spec."""
        return self.requirements_path.exists()

    @property
    def capabilities(self) -> WorkspaceCapabilities:
        """Get the capabilities for the workspace"""
        return {
            "ingest": self.has_sources,
            "transform": self.has_transforms,
            "publish": self.has_publishers,
            "deps": self.has_dependencies,
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
        if not self.has_dependencies:
            return Path(sys.executable)
        return self.root / ".venv" / "bin" / "python"

    @property
    def pip_path(self) -> Path:
        """Get path to workspace pip.

        Returns:
            Path to pip executable. If there is no requirements.txt, returns the most probable system pip
        """
        if not self.has_dependencies:
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
        if self.has_dependencies and not self.python_path.exists():
            return self.setup_venv()

    def setup_venv(self) -> None:
        """Create a virtual environment. Clear and reinstantiate env if it exists.

        The canonical route to this method is via ensure_venv, but developers can call this
        directly for more control or override the implementation in a Workspace subclass.

        Raises:
            SubprocessError if pip fails to install the requirements.txt
        """
        virtualenv.cli_run([str(self.root / ".venv")])
        subprocess.check_call([self.pip_path, "install", "-r", self.requirements_path])

    @contextmanager
    def activate_venv(self) -> t.Iterator[None]:
        """Activate the workspace virtual environment. A noop if there are no deps.

        This method is a context manager that activates the workspace virtual environment. It
        does so in the context of the current interpreter.
        """
        if not self.has_dependencies:
            yield
            return
        activate = self.root / ".venv" / "bin" / "activate_this.py"
        environ_backup = os.environ.copy()
        syspath_backup = sys.path.copy()
        sysprefix_backup = sys.prefix
        exec(activate.read_bytes(), {"__file__": str(activate)})
        yield
        os.environ = environ_backup
        sys.path = syspath_backup
        sys.prefix = sysprefix_backup

    def inject_workspace_config_providers(self, as_: str | None = None, /) -> None:
        """Inject workspace config into context

        Args:
            as_ (str): The name to inject the workspace as.
        """
        from cdf.core.config import config_provider_factory, inject_config_providers

        if self._did_inject_config_providers:
            return

        if as_ is None:
            as_ = self.root.name
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

    def load_sources(self) -> t.Dict[str, WorkspaceSourceContext]:
        """Load sources from workspace.

        This method loads all sources from the workspace and returns a dict of source metadata. It
        does this by adding the workspace to the sys.path and importing all modules in the sources
        directory. It then looks for the __CDF_SOURCE__ attribute on each module and adds the
        metadata to the cache. If the workspace has dependencies, it creates a virtual environment
        and adds the workspace venv to the sys.path. This ensures that all dependencies are
        available to the source modules.
        """
        if not self.has_sources:
            return {}
        if self.has_dependencies:
            self.ensure_venv()
        if not self._cached_sources:
            with _IMPORT_LOCK, augmented_path(
                str(self.root / c.SOURCES_PATH)
            ), self.activate_venv():
                sources = {}
                for path in self.source_paths:
                    # GOALS:
                    # Load module with virtualenv activated within the context of an existing process
                    # Execute the code and capture the __CDF_SOURCE__ attribute
                    # Capture the globals and locals such that we can execute the deferred_fn later in the same context
                    mod_globals = {"__name__": "__main__", "__file__": str(path)}
                    mod_locals = {}
                    exec(
                        compile(path.read_text(), path, "exec"),
                        mod_globals,
                        mod_locals,
                    )
                    mod_globals.update(mod_locals)
                    for src, spec in mod_locals.get(c.CDF_SOURCE, {}).items():
                        sources[src] = {
                            "spec": spec,
                            "globals": mod_globals,
                            "locals": mod_locals,
                        }
            self._cached_sources.update(sources)
        return self._cached_sources

    def __getitem__(self, name: str) -> WorkspaceSourceContext:
        """Get a source by name."""
        return self.load_sources()[name]

    def sandbox(
        self, src: str
    ) -> t.Generator[CDFSource, Pipeline, t.Callable[..., LoadInfo]]:
        ctx = self[src]
        source = eval(
            f"{c.CDF_SOURCE}['{src}'].deferred_fn()", ctx["globals"], ctx["locals"]
        )

        # We grab source from sandbox and ask for a pipeline in order to run it
        # in the same sandbox. Yielding the source to the caller allows
        # them to manipulate the source before sending a pipeline back.
        pipeline = yield source

        # Now we prepare the sandbox for execution and let the caller run it.
        # this function will execute the source in the sandbox
        ctx["locals"].update({"__source__": source, "__pipe__": pipeline})

        def run(*runargs, **runkwargs) -> LoadInfo:
            """Closure that runs the source in the sandbox."""
            return eval(
                "__pipe__.run(__source__, *__runargs, **__runkwargs)",
                ctx["globals"],
                {
                    **ctx["locals"],
                    "__runargs": runargs,
                    "__runkwargs": runkwargs,
                },
            )

        return run

    def get_extractor(self, src: str, pipeline: Pipeline) -> t.Callable[..., LoadInfo]:
        """Get an extract function for a source.

        This method takes a configured pipeline and returns a function that can be called to
        extract data from a source. It will automatically run in a sandboxed environment with
        dependencies available.
        """
        source = next(sandbox := self.sandbox(src))
        try:
            sandbox.send(pipeline)
        except StopIteration as rv:
            return rv.value
        else:
            raise RuntimeError("Source %s failed to initialize", source)
