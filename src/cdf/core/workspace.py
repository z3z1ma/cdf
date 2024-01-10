"""Continuous data framework workspaces and projects"""
import getpass
import os
import subprocess
import sys
import typing as t
import warnings
from contextlib import contextmanager
from functools import cache, wraps
from pathlib import Path
from threading import Lock
from types import MappingProxyType

import dlt
import dlt.common.configuration.providers as providers
import dotenv
import sqlmesh
import tomlkit
from dlt.common.configuration.container import Container
from dlt.common.configuration.specs.config_providers_context import (
    ConfigProvidersContext,
)

import cdf.core.constants as c
import cdf.core.context as context
import cdf.core.feature_flags as ff
import cdf.core.jinja as jinja
from cdf.core.spec import (
    CDF_REGISTRY,
    CDFModelLoader,
    ComponentSpecification,
    PipelineSpecification,
    PublisherSpecification,
    ScriptSpecification,
    SinkSpecification,
)

_LOADING_MUTEX: Lock = Lock()
"""A lock which ensures that operations such as component loading are atomic."""


def _find_git_root(*paths: str | Path) -> Path | None:
    """
    Finds the common git root.

    Args:
        *paths: The paths to search.

    Returns:
        Path: The common git root or None.
    """
    roots = []
    for path in paths:
        path = Path(path).resolve()
        for parent in [path] + list(path.parents):
            if (parent / ".git").is_dir():
                roots.append(parent)
    if not roots or len(set(roots)) != 1:
        return None
    return roots[0]


def _find_common_path(*paths: str | Path) -> Path | None:
    """
    Finds the common path for a list of paths.

    Args:
        *paths: The paths to search.

    Returns:
        Path: The common path or None.
    """
    if not paths:
        return None
    common_path = Path(os.path.commonpath([Path(p).resolve() for p in paths]))
    if not common_path.parents:
        return None
    return common_path


def _coerce_to_workspace(obj: "str | Path | Workspace") -> "Workspace":
    """
    Get a workspace from a workspace-like object.

    Args:
        workspace: The path to the workspace.

    Raises:
        TypeError: If object is not coercible to a workspace.

    Returns:
        Workspace: A workspace.
    """
    if isinstance(obj, (str, Path)):
        return Workspace(obj)
    elif isinstance(obj, Workspace):
        return obj
    else:
        raise TypeError(f"Expected PathLike or Workspace, got {type(obj)}")


class Project(t.Dict["str", "Workspace"]):
    """A project encapsulates a collection of workspaces."""

    class TOMLSpec(t.TypedDict):
        """A project specification."""

        name: str | None
        """The name of the project."""
        members: t.List[str]
        """A list of workspace paths."""
        sinks: t.List[dict]
        """Global sinks."""

    def __init__(
        self,
        *members: "str | Path | Workspace",
        name: str | None,
        root: Path | None = None,
        setup: bool = True,
    ) -> None:
        """
        Initialize a project.

        Args:
            *members: The paths to the workspaces.
            name: The name of the project.
            root: The root of the project. Defaults to None.
            setup: Whether to load the .env and update sys.path. Defaults to True.
        """
        if not name:
            warnings.warn(
                "Project name not provided -- we will infer it from the project members but it is"
                " highly recommended that you provide one explicitly to avoid unexpected behavior.",
                category=UserWarning,
            )
        self._root = root
        self._name = name
        self._seq = context.get_project_number()

        ws = [_coerce_to_workspace(w) for w in members]
        super().__init__({w.name: w for w in ws})

        if setup:
            self.setup()

    @classmethod
    def default(cls, path: str | Path | None = None) -> "Project":
        """
        Create a project assuming a single workspace at the given path or cwd.

        Args:
            path: The path to the workspace. Defaults to None.

        Raises:
            ValueError: If path is supplied but does not exist.

        Returns:
            Project: A project.
        """
        if path is None:
            path = Path.cwd()
        elif isinstance(path, str):
            path = Path(path).expanduser().resolve()
        workspace = Workspace.find_nearest(path, must_exist=True)
        return cls(
            workspace,
            name=c.DEFAULT_WORKSPACE,
            root=workspace.root,
        )

    @classmethod
    def from_spec(cls, spec: "Project.TOMLSpec", root: Path | None = None) -> "Project":
        """
        Create a project from a TOML spec.

        Args:
            spec: A Project TOML spec.
            root: The root of the project. Defaults to None.

        Returns:
            Project: A project.
        """
        project = cls(*spec["members"], name=spec.get("name"), root=root)
        if global_sinks := spec.get("sinks"):
            for workspace in project.values():
                workspace.config_dict.setdefault(
                    c.SPECS,
                    {},
                ).setdefault(
                    c.SINKS,
                    [],
                ).extend(global_sinks)
        return project

    @classmethod
    def from_spec_path(cls, config_path: Path | str) -> "Project":
        """
        Create a project from a cdf_project.toml file.

        This is the canonical way to create a project. The workspace.toml file is a TOML file
        that contains a [workspace] section with a members key. The members key is a list of
        member specs. For example:

        ```toml
        [project]
        name = "my_project"
        members = [
            "workspace/data",
            "workspace/marketing"
        ]

        [[sinks]]
        name = "global-sink"
        entrypoint = "common.sinks:postgres"
        ```

        Args:
            path (Path): Path to workspace.toml.

        Returns:
            Project: A project.
        """
        if isinstance(config_path, str):
            config_path = Path(config_path).expanduser().resolve()
        if not config_path.exists():
            raise ValueError(f"Could not find TOML at {config_path}")
        dotenv.load_dotenv(config_path.parent / ".env")
        with config_path.open("r") as f:
            conf: Project.TOMLSpec = (
                tomlkit.loads(jinja.render(f.read()))
                .unwrap()
                .get("project", {"name": None, "members": []})
            )
        return cls.from_spec(conf, root=config_path.parent)

    @classmethod
    def find_nearest(
        cls, path: Path | str | None = None, must_exist: bool = False
    ) -> "Project":
        """
        Find nearest project.

        If no cdf_workspace.toml file is found, returns the current working directory as a project.
        This populates the meta attribute of the project with the path to the project root.

        Args:
            path: The path to search from.
            must_exist: Whether to raise an error if no project is found.

        Raises:
            ValueError: If no project is found and must_exist is True.

        Returns:
            Project: A project.
        """
        if path is None:
            path = Path.cwd()
        elif isinstance(path, str):
            path = Path(path).expanduser().resolve()
        orig_path = path
        while path.parents:
            workspace_spec = path / c.PROJECT_FILE
            if workspace_spec.exists():
                return cls.from_spec_path(workspace_spec)
            path = path.parent
        if must_exist:
            raise ValueError(
                f"Could not find a project TOML in {path} or any of its parents"
            )
        return cls.default(orig_path)

    @property
    def root(self) -> Path:
        """Get the root of the project. Infer from workspaces if not set."""
        if self._root:
            return self._root
        return self._infer_root() or Path.cwd()

    @property
    def name(self) -> str:
        """Get the name of the project. Infer from workspaces if not set."""
        if self._name:
            return self._name
        return f"project_{self._seq}"

    def _infer_root(self) -> Path | None:
        """
        Infer the root of the project.

        If there is 1 workspace, use it as the root. If there are multiple workspaces, prefer
        a common git root if possible, otherwise a common path. If there is no common path or
        ambiguous git root, return None.
        """
        if len(self) == 1:
            return next(iter(self.values())).root
        gitpath = _find_git_root(*[w.root for w in self.values()])
        if gitpath:
            return gitpath
        if len(self) > 1:
            commonpath = _find_common_path(*[w.root for w in self.values()])
            if commonpath and commonpath != commonpath.root:
                return commonpath
        return None

    def setup(self) -> None:
        """Load the .env and update sys.path such that all workspaces are importable."""
        dotenv.load_dotenv(self.root / ".env")
        sys.path.append(str(self.root))
        for ws in self.values():
            sys.path.append(str(ws.root.parent))

    def add_workspace(self, workspace: "Workspace") -> None:
        """Add a workspace to the project."""
        self[workspace.name] = workspace

    def remove_workspace(self, name: str) -> None:
        """Remove a workspace from the project."""
        del self[name]

    def transform_context(
        self, *workspaces: str, sink: str | None = None, load: bool = True
    ) -> sqlmesh.Context:
        """
        Get a sqlmesh context for a list of workspaces.

        Args:
            workspaces: Names of workspaces to include in the context. The first workspace
                is used as the root.
            sink: Name of transform gateway. Defaults to None.

        Returns:
            sqlmesh.Context: A sqlmesh context.
        """
        return sqlmesh.Context(
            config={
                self[workspace].root: self[workspace].transform_config(sink)
                for workspace in workspaces
            },
            load=load,
        )

    def __getitem__(self, name: str) -> "Workspace":
        """Get a workspace by name."""
        try:
            workspace = super().__getitem__(name)
        except KeyError as e:
            raise KeyError(f"Workspace {name} not found in project {self.name}") from e
        context.set_active_workspace(workspace)
        workspace.config.activate()
        return workspace

    def __setitem__(self, name: str, workspace: "Workspace") -> None:
        """Add a workspace to the project. The workspace name must match the key."""
        if not isinstance(workspace, Workspace):
            raise ValueError(f"Expected Workspace, got {type(workspace)}")
        try:
            workspace.root.relative_to(self.root)
        except ValueError as e:
            raise ValueError(
                f"Workspace {workspace.name} is not a member (subdir) of project {self.name}"
            ) from e
        if name != workspace.name:
            raise KeyError(
                f"Workspace name {workspace.name} does not match key {name} in setitem call"
            )
        super().__setitem__(name, workspace)

    def __delitem__(self, name: str) -> None:
        """Remove a workspace from the project."""
        try:
            self.pop(name).clear()
        except KeyError as e:
            raise KeyError(f"Workspace {name} not found in project {self.name}") from e

    def __getattr__(self, name: str) -> "Workspace":
        """Get a workspace by name."""
        try:
            return self[name]
        except KeyError as e:
            raise AttributeError(
                f"Workspace {name} not found in project {self.name}"
            ) from e

    def __delattr__(self, name: str) -> None:
        """Remove a workspace from the project."""
        try:
            del self[name]
        except KeyError as e:
            raise AttributeError(
                f"Workspace {name} not found in project {self.name}"
            ) from e

    def __repr__(self) -> str:
        """Get a string representation of the project."""
        workspaces = list(self.values())
        return f"Project('{self.name}', {workspaces=})"


V = t.TypeVar("V", bound=ComponentSpecification | sqlmesh.Model)


class WorkspaceRegistryProxy(t.Dict[str, V]):
    """A mapping proxy that allows component access via dict or attrs."""

    def __getattr__(self, name: str) -> V:
        try:
            return self[name]
        except KeyError as e:
            raise AttributeError(f"Component {name} not found in workspace") from e

    def __setattr__(self, name: str, value: V) -> None:
        raise AttributeError("Cannot set attributes on a workspace registry proxy")

    def __delattr__(self, name: str) -> None:
        raise AttributeError("Cannot delete attributes on a workspace registry proxy")

    def __setitem__(self, name: str, value: V) -> None:
        raise TypeError("Cannot set items on a workspace registry proxy")

    def __delitem__(self, name: str) -> None:
        raise TypeError("Cannot delete items on a workspace registry proxy")


P = t.ParamSpec("P")
T = t.TypeVar("T", bound=t.Callable[..., t.Any])


def lazy_load(*types: t.Type[ComponentSpecification]) -> t.Callable[[T], T]:
    """Decorator to lazy load a particular component type from the workspace."""

    def decorator(fn: t.Callable[P, T]) -> T:
        @wraps(fn)
        def wrapper(*args: P.args, **kwargs: P.kwargs):
            t.cast("Workspace", args[0]).load(*types)
            return fn(*args, **kwargs)

        return t.cast(T, wrapper)

    return decorator


class Workspace:
    """A workspace encapsulates a directory containing pipelines, publishers, metadata, and models.

    We can think of a Workspace as a pathlib.Path with properties based on a configuration file in
    the workspace root. Therefore a workspace can be serialized as tarball and deserialized as a
    workspace. This is useful for packaging and deploying workspaces.
    """

    ROOT_MARKERS: t.ClassVar[t.List[str]] = [
        ".git",
        c.CONFIG_FILE,
    ]

    class ConfigProvider(providers.StringTomlProvider):
        """Provider for CDF which reads from a TOML file rendering it with the CDF jinja environment."""

        def __init__(self, workspace: "Workspace") -> None:
            """Initialize a config provider."""
            self._name = workspace.name
            config_path = workspace.root / c.CONFIG_FILE
            if not config_path.is_file():
                raise FileNotFoundError(f"Config file {config_path} not found.")
            with config_path.open("r", encoding="utf-8") as f:
                super().__init__(jinja.render(f.read()))
            self.data = self._toml.unwrap()

        def __getitem__(self, key: str) -> t.Any:
            """Get a config value."""
            self.activate()
            return dlt.config[key]

        def activate(self) -> None:
            """Set the config instance as the active config."""
            ctx = Container()[ConfigProvidersContext]
            ctx.providers = [providers.EnvironProvider(), self]

        def deactivate(self) -> None:
            """Remove the config instance from the active config."""
            ctx = Container()[ConfigProvidersContext]
            ctx.providers = [providers.EnvironProvider()]

        @property
        def name(self) -> str:
            """Get the name of the config."""
            return self._name

    def __init__(
        self,
        root: str | Path,
        mkdir: bool = False,
        load: bool = False,
    ) -> None:
        """
        Initialize a workspace.

        Args:
            root: The root of the workspace.
            mkdir: Whether to create the workspace if it does not exist.
                Defaults to False.
            load: Whether to load the workspace components eagerly. Defaults to False.

        Raises:
            ValueError: If the workspace does not exist and mkdir is False.

        Returns:
            Workspace: A workspace.
        """
        self.root = Path(root).expanduser().resolve()
        if not self.root.exists():
            if mkdir:
                self.root.mkdir(parents=True)
            else:
                raise ValueError(
                    f"Tried to init Workspace with nonexistent path {self.root}"
                )

        self.registry = MappingProxyType(CDF_REGISTRY[self.name])
        self.config = Workspace.ConfigProvider(self)
        self._loaded: t.Dict[t.Type[ComponentSpecification], bool] = {}

        if load:
            self.load()

    @classmethod
    def find_nearest(
        cls, path: str | Path | None = None, must_exist: bool = False
    ) -> "Workspace":
        """
        Find nearest workspace recursing up the directory tree.

        Args:
            path: Path to search from. Defaults to the current working directory.
            must_exist: Whether to raise an error if no workspace is found. Defaults to False.

        Raises:
            ValueError: If no workspace is found and must_exist is True.

        Returns:
            Workspace: The nearest workspace.
        """
        if path is None:
            path = Path.cwd()
        elif isinstance(path, str):
            path = Path(path).expanduser().resolve()

        while path.parents:
            if any((path / marker).exists() for marker in cls.ROOT_MARKERS):
                return cls(path)
            path = path.parent

        if must_exist:
            raise ValueError(
                f"Could not find a workspace root in {path} or any of its parents"
            )

        return cls(path)

    @property
    def name(self) -> str:
        """Get the namespace of the workspace."""
        return self.root.name

    @property
    def config_dict(self) -> t.Dict[str, t.Any]:
        """Get the configuration for the workspace."""
        return self.config.data

    def load(self, *types: t.Type[ComponentSpecification]) -> None:
        """Load the components in the workspace."""
        if not types:
            types = (
                PipelineSpecification,
                PublisherSpecification,
                SinkSpecification,
                ScriptSpecification,
            )
        specs = self.config_dict.get(c.SPECS, {})
        for typ in types:
            if self._loaded.get(typ, False):
                continue

            with _LOADING_MUTEX, self.runtime_context():
                for comp in specs.get(typ._key.default, []):  # type: ignore
                    typ(**comp)
            self._loaded[typ] = True

    def clear(self) -> None:
        """Clear the components in the workspace."""
        self.registry[c.PIPELINES].clear()
        self.registry[c.PUBLISHERS].clear()
        self.registry[c.SINKS].clear()
        self.registry[c.SCRIPTS].clear()
        self.config.deactivate()
        self._loaded.clear()

    @property
    @lazy_load(PipelineSpecification)
    def pipelines(self) -> WorkspaceRegistryProxy[PipelineSpecification]:
        """Get the pipelines in the workspace."""
        context.set_active_workspace(self)
        self.config.activate()
        return WorkspaceRegistryProxy(self.registry[c.PIPELINES])

    @property
    @lazy_load(PublisherSpecification)
    def publishers(self) -> WorkspaceRegistryProxy[PublisherSpecification]:
        """Get the publishers in the workspace."""
        context.set_active_workspace(self)
        self.config.activate()
        return WorkspaceRegistryProxy(self.registry[c.PUBLISHERS])

    @property
    @lazy_load(SinkSpecification)
    def sinks(self) -> WorkspaceRegistryProxy[SinkSpecification]:
        """Get the sinks in the workspace."""
        context.set_active_workspace(self)
        self.config.activate()
        return WorkspaceRegistryProxy(self.registry[c.SINKS])

    @property
    @lazy_load(ScriptSpecification)
    def scripts(self) -> WorkspaceRegistryProxy[ScriptSpecification]:
        """Get the scripts in the workspace."""
        context.set_active_workspace(self)
        self.config.activate()
        return WorkspaceRegistryProxy(self.registry[c.SCRIPTS])

    @property
    def models(self) -> WorkspaceRegistryProxy[sqlmesh.Model]:
        """Load transforms from workspace using the first available context."""
        transform = self.transform_context(next(iter(self.sinks)))
        return WorkspaceRegistryProxy(transform.models)

    @contextmanager
    def runtime_context(self) -> t.Iterator[None]:
        """Runtime context for a workspace."""
        origcontext = context.get_active_workspace()
        origsyspath = sys.path

        context.set_active_workspace(self)
        sys.path.insert(0, str(self.root))

        self.config.activate()
        yield
        self.config.deactivate()

        sys.path = origsyspath
        context.set_active_workspace(origcontext)

    @cache
    def transform_config(self, sink_name: str, **opts: t.Any) -> sqlmesh.Config:
        """
        Create a transform config for this workspace.

        Args:
            sink: The name of the sink to use.

        Returns:
            sqlmesh.Config: The transform config.
        """
        if sink_name not in self.sinks:
            raise ValueError(f"Sink {sink_name} not found in workspace {self.name}")
        sink = self.sinks[sink_name]
        gateway = sink("gateway")
        if gateway is None:
            raise ValueError(f"Sink {sink_name} does not have a gateway configured.")
        git_branch_proc = subprocess.run(
            ["git", "rev-parse", "--abbrev-ref", "HEAD"],
            capture_output=True,
            cwd=self.root,
        )
        if git_branch_proc.returncode != 0:
            default_target_environment = "prod"
        else:
            git_branch = git_branch_proc.stdout.decode("utf-8").strip()
            if git_branch in ["master", "main"]:
                default_target_environment = "prod"
            else:
                default_target_environment = git_branch or "dev"
        transform_opts = self.config_dict.get(c.TRANSFORM_SPEC, {}) | opts
        conf = sqlmesh.Config(
            **transform_opts,
            gateways={sink.name: gateway},
            default_gateway=sink.name,
            project=self.name,
            loader=CDFModelLoader,
            loader_kwargs=dict(sink=sink_name),
            username=os.getenv("CDF_USER", getpass.getuser()),
            default_target_environment=default_target_environment,
            notification_targets=[
                {  # type: ignore
                    "type": "slack_webhook",
                    "url": "...",
                    "notify_on": [
                        "apply_start",
                        "apply_end",
                        "apply_failure",
                        "run_start",
                        "run_end",
                        "run_failure",
                        "audit_failure",
                    ],
                }
            ],
        )
        return conf

    @cache
    def transform_context(self, sink: str, load: bool = True) -> sqlmesh.Context:
        """
        Create a transform context for this sink.

        Args:
            sink: The name of the sink to use.

        Returns:
            sqlmesh.Context: The transform context.
        """
        return sqlmesh.Context(
            config={self.root: self.transform_config(sink)}, load=load
        )

    @contextmanager
    def runtime_source(
        self, pipeline_name: str, **kwargs
    ) -> t.Iterator[dlt.sources.DltSource]:
        """
        Get a runtime source from the workspace.

        A runtime source is a the equivalent to the source cdf would generate itself in a typical
        pipeline execution. The source is pulled out from the cooperative pipeline interface.

        Args:
            pipeline_name: Name of source to get.
            **kwargs: Keyword args to pass to pipeline function if it is a callable.
        """
        with self.runtime_context():
            yield ff.process_source(
                next(self.pipelines[pipeline_name].unwrap(**kwargs)),
                ff.get_provider(self),
            )

    def __repr__(self) -> str:
        """Get a string representation of the workspace."""
        specs = self.config_dict.get(c.SPECS, {})
        root = self.root.relative_to(Path.cwd())
        pipelines = [p["entrypoint"] for p in specs.get(c.PIPELINES, [])]
        publishers = [p["entrypoint"] for p in specs.get(c.PUBLISHERS, [])]
        sinks = [p["entrypoint"] for p in specs.get(c.SINKS, [])]
        scripts = [p["entrypoint"] for p in specs.get(c.SCRIPTS, [])]
        return f"Workspace('{self.name}', {root=}, {pipelines=}, {publishers=}, {sinks=}, {scripts=})"

    def __getattr__(self, name: str) -> t.Any:
        """Proxy to the pathlib.Path obj so a workspace behaves like an augmented pathlib.Path."""
        if hasattr(self.root, name):
            return getattr(self.root, name)
        raise AttributeError(f"Workspace has no attribute {name}")

    def __getstate__(self) -> t.Dict[str, t.Any]:
        """Get the state of the workspace."""
        return {
            "root": self.root.as_posix(),
            "_config": self.config.dumps(),
        }

    def __setstate__(self, state: t.Dict[str, t.Any]) -> None:
        """Set the state of the workspace."""
        self.__init__(state["root"])
        self.config.loads(state["_config"])

    if t.TYPE_CHECKING:
        as_posix = Path.as_posix
        as_uri = Path.as_uri
        drive = Path.drive
        exists = Path.exists
        is_dir = Path.is_dir
        iterdir = Path.iterdir
        match = Path.match
        mkdir = Path.mkdir
        parent = Path.parent
        parents = Path.parents
        relative_to = Path.relative_to
        rename = Path.rename
        replace = Path.replace
        resolve = Path.resolve
