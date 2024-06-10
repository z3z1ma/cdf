"""The project module provides a way to define a project and its workspaces.

Everything in CDF is described via a simple configuration structure. We parse this configuration
using dynaconf which provides a simple way to load configuration from various sources such as
environment variables, YAML, TOML, JSON, and Python files. It also provides many other features
such as loading .env files, env-specific configuration, templating via @ tokens, and more. The
configuration is then validated with pydantic to ensure it is correct and to give us well defined
types to work with. The underlying dynaconf settings object is stored in the `wrapped` attribute
of the Project and Workspace settings objects. This allows us to access the raw configuration
values if needed. ChainMaps are used to provide a scoped view of the configuration. This enables
a powerful layering mechanism where we can override configuration values at different levels.
Finally, we provide a context manager to inject the project configuration into the dlt context
which allows us to access the configuration throughout the dlt codebase and in data pipelines.

Example:

```toml
# cdf.toml
[default]
name = "cdf-example"
workspaces = ["alex"]
filesystem.uri = "file://_storage"
feature_flags.provider = "filesystem"
feature_flags.filename = "feature_flags.json"

[prod]
filesystem.uri = "gcs://bucket/path"
```

```toml
# alex/cdf.toml
[pipelines.us_cities] # alex/pipelines/us_cities_pipeline.py
version = 1
dataset_name = "us_cities_v0_{version}"
description = "Get US city data"
options.full_refresh = false
options.runtime.dlthub_telemetry = false
```
"""

import itertools
import os
import time
import typing as t
from collections import ChainMap
from contextlib import contextmanager, suppress
from enum import Enum
from functools import cached_property
from pathlib import Path

import duckdb
import dynaconf
import pydantic
from dynaconf.utils.boxing import DynaBox
from dynaconf.vendor.box import Box

import cdf.core.constants as c
import cdf.core.specification as spec
from cdf.core.config import inject_configuration
from cdf.core.feature_flag import (
    AbstractFeatureFlagAdapter,
    get_feature_flag_adapter_cls,
)
from cdf.core.filesystem import FilesystemAdapter
from cdf.core.state import StateStore
from cdf.types import M, PathLike

if t.TYPE_CHECKING:
    from sqlmesh.core.config import GatewayConfig

T = t.TypeVar("T")


class _BaseSettings(pydantic.BaseModel):
    """A base model for CDF settings"""

    model_config = pydantic.ConfigDict(
        frozen=True,
        use_attribute_docstrings=True,
        from_attributes=True,
        populate_by_name=True,
    )

    _generation: float = pydantic.PrivateAttr(default_factory=time.monotonic)
    """A monotonic timestamp of when the model was generated"""

    def __hash__(self) -> int:
        return hash(self.model_dump_json())

    def __eq__(self, other: t.Any) -> bool:
        if not isinstance(other, type(self)):
            return False
        return self.model_dump() == other.model_dump()

    def is_newer_than(self, other: "Project") -> bool:
        """Check if the model is newer than another model"""
        return self._generation > other._generation

    def is_older_than(self, other: "Project") -> bool:
        """Check if the model is older than another model"""
        return self._generation < other._generation

    def model_dump(self, **kwargs: t.Any) -> t.Dict[str, t.Any]:
        """Dump the model to a dictionary"""
        kwargs.setdefault("by_alias", True)
        return super().model_dump(**kwargs)


class FilesystemConfig(_BaseSettings):
    """Configuration for a filesystem provider"""

    uri: str = "_storage"
    """The filesystem URI

    This is based on fsspec. See https://filesystem-spec.readthedocs.io/en/latest/index.html
    This supports all filesystems supported by fsspec as well as filesystem chaining.
    """
    options_: t.Annotated[
        t.Tuple[t.Tuple[str, t.Any], ...], pydantic.Field(alias="options")
    ] = ()
    """The filesystem options

    Options are passed to the filesystem provider as keyword arguments.
    """

    _project: t.Optional["Project"] = None
    """The project this configuration belongs to"""

    @pydantic.field_validator("options_", mode="before")
    @classmethod
    def _options_validator(cls, value: t.Any) -> t.Any:
        """Convert the options to an immutable tuple of tuples"""
        if isinstance(value, dict):
            value = tuple(value.items())
        return value

    @property
    def options(self) -> t.Dict[str, t.Any]:
        """Get the filesystem options as a dictionary"""
        return dict(self.options_)

    @property
    def project(self) -> "Project":
        """Get the project this configuration belongs to"""
        if self._project is None:
            raise ValueError("Filesystem configuration not associated with a project")
        return self._project

    @property
    def has_project_association(self) -> bool:
        """Check if the configuration is associated with a project"""
        return self._project is not None

    def get_adapter(self) -> M.Result[FilesystemAdapter, Exception]:
        """Get a filesystem adapter"""
        if self.has_project_association:
            root = self.project.path
        else:
            root = None
        try:
            return M.ok(FilesystemAdapter(self.uri, root, self.options))
        except Exception as e:
            return M.error(e)


class FeatureFlagProviderType(str, Enum):
    """The feature flag provider"""

    FILESYSTEM = "filesystem"
    HARNESS = "harness"
    LAUNCHDARKLY = "launchdarkly"
    SPLIT = "split"
    NOOP = "noop"


class BaseFeatureFlagConfig(_BaseSettings):
    """Base configuration for a feature flags provider"""

    provider: FeatureFlagProviderType
    """The feature flags provider"""

    _project: t.Optional["Project"] = None
    """The project this configuration belongs to"""

    @property
    def project(self) -> "Project":
        """Get the project this configuration belongs to"""
        if self._project is None:
            raise ValueError("Feature flag configuration not associated with a project")
        return self._project

    @property
    def has_project_association(self) -> bool:
        """Check if the configuration is associated with a project"""
        return self._project is not None

    def get_adapter(
        self, **kwargs: t.Any
    ) -> M.Result[AbstractFeatureFlagAdapter, Exception]:
        """Get a handle to the feature flag adapter"""
        options = self.model_dump()
        provider = str(options.pop("provider").value)
        options.update(kwargs)
        return get_feature_flag_adapter_cls(provider).map(
            lambda cls: cls(**options, filesystem=self.project.fs_adapter.wrapped)
        )


class FilesystemFeatureFlagConfig(BaseFeatureFlagConfig):
    """Configuration for a feature flags provider that uses the configured filesystem"""

    provider: t.Literal[FeatureFlagProviderType.FILESYSTEM] = (
        FeatureFlagProviderType.FILESYSTEM
    )
    """The feature flags provider"""
    filename: str = "feature_flags.json"
    """The feature flags filename.

    This is a format string that can include the following variables:
    - `name`: The project name
    - `workspace`: The workspace name
    - `environment`: The environment name
    - `source`: The source name
    - `resource`: The resource name
    - `version`: The version number of the component
    """


class HarnessFeatureFlagConfig(BaseFeatureFlagConfig):
    """Configuration for a feature flags provider that uses the Harness API"""

    provider: t.Literal[FeatureFlagProviderType.HARNESS] = (
        FeatureFlagProviderType.HARNESS
    )
    """The feature flags provider"""
    api_key: str = pydantic.Field(
        os.getenv("HARNESS_API_KEY", ...),
        pattern=r"^[ps]at\.[a-zA-Z0-9_\-]+\.[a-fA-F0-9]+\.[a-zA-Z0-9_\-]+$",
    )
    """The harness API key. Get it from your user settings"""
    sdk_key: pydantic.UUID4 = pydantic.Field(os.getenv("HARNESS_SDK_KEY", ...))
    """The harness SDK key. Get it from the environment management page of the FF module"""
    account: str = pydantic.Field(
        os.getenv("HARNESS_ACCOUNT_ID", ...),
        min_length=22,
        max_length=22,
        pattern=r"^[a-zA-Z0-9_\-]+$",
    )
    """The harness account ID. We will attempt to read it from the environment if not provided."""
    organization: str = pydantic.Field(os.getenv("HARNESS_ORG_ID", "default"))
    """The harness organization ID. We will attempt to read it from the environment if not provided."""
    project_: str = pydantic.Field(
        os.getenv("HARNESS_PROJECT_ID", ...), alias="project"
    )
    """The harness project ID. We will attempt to read it from the environment if not provided."""


class LaunchDarklyFeatureFlagSettings(BaseFeatureFlagConfig):
    """Configuration for a feature flags provider that uses the LaunchDarkly API"""

    provider: t.Literal[FeatureFlagProviderType.LAUNCHDARKLY] = (
        FeatureFlagProviderType.LAUNCHDARKLY
    )
    """The feature flags provider"""
    api_key: str = pydantic.Field(
        os.getenv("LAUNCHDARKLY_API_KEY", ...),
        pattern=r"^[a-zA-Z0-9_\-]+$",
    )
    """The LaunchDarkly API key. Get it from your user settings"""


class SplitFeatureFlagSettings(BaseFeatureFlagConfig):
    """Configuration for a feature flags provider that uses the Split API"""

    provider: t.Literal[FeatureFlagProviderType.SPLIT] = FeatureFlagProviderType.SPLIT
    """The feature flags provider"""
    api_key: str = pydantic.Field(
        os.getenv("SPLIT_API_KEY", ...),
        pattern=r"^[a-zA-Z0-9_\-]+$",
    )
    """The Split API key. Get it from your user settings"""


class NoopFeatureFlagSettings(BaseFeatureFlagConfig):
    """Configuration for a feature flags provider that does nothing"""

    provider: t.Literal[FeatureFlagProviderType.NOOP] = FeatureFlagProviderType.NOOP
    """The feature flags provider"""


FeatureFlagConfig = t.Union[
    FilesystemFeatureFlagConfig,
    HarnessFeatureFlagConfig,
    LaunchDarklyFeatureFlagSettings,
    SplitFeatureFlagSettings,
    NoopFeatureFlagSettings,
]
"""A union of all feature flag provider configurations"""


class Workspace(_BaseSettings):
    """A workspace is a collection of pipelines, sinks, publishers, scripts, and notebooks in a subdirectory of the project"""

    workspace_path: t.Annotated[Path, pydantic.Field(alias="path")] = Path(".")
    """The path to the workspace within the project path"""
    project_path: Path = Path(".")
    """The path to the project"""
    name: t.Annotated[
        str, pydantic.Field(pattern=r"^[a-zA-Z0-9_\-]+$", min_length=3, max_length=32)
    ] = "default"
    """The name of the workspace"""
    owner: t.Optional[str] = None
    """The owner of the workspace"""
    pipelines: t.Tuple[spec.PipelineSpecification, ...] = ()
    """Pipelines move data from sources to sinks"""
    sinks: t.Tuple[spec.SinkSpecification, ...] = ()
    """A sink is a destination for data"""
    publishers: t.Tuple[spec.PublisherSpecification, ...] = ()
    """Publishers send data to external systems"""
    scripts: t.Tuple[spec.ScriptSpecification, ...] = ()
    """Scripts are used to automate tasks"""
    notebooks: t.Tuple[spec.NotebookSpecification, ...] = ()
    """Notebooks are used for data analysis and reporting"""

    _project: t.Optional["Project"] = None
    """The project this workspace belongs to. Set by the project model validator."""

    @pydantic.field_validator(
        "pipelines", "sinks", "publishers", "scripts", "notebooks", mode="before"
    )
    @classmethod
    def _workspace_component_validator(
        cls, value: t.Any, info: pydantic.ValidationInfo
    ):
        """Parse component dictionaries into an array of components inject the workspace path"""
        if isinstance(value, dict):
            # name : {config}
            cmps = []
            for key, cmp in value.items():
                if isinstance(cmp, (str, Path)):
                    # name : path
                    cmp = {"path": cmp}
                cmp.setdefault("name", key)
                cmps.append(cmp)
            value = cmps
        elif hasattr(value, "__iter__") and not isinstance(value, (str, bytes)):
            # [{configA}, ...]
            value = list(value)
        else:
            raise ValueError(
                "Invalid workspace component configuration, must be either a dict or a list"
            )
        for cmp in value:
            # TODO: gut check this, its interesting how the tree-like structure
            # of project -> workspace -> component requires us to bubble down
            # the accumulated path since each layer is a separate model validator
            # and component validator ultimately relies on a fully resolvable path
            cmp["root_path"] = Path(
                info.data["project_path"], info.data["workspace_path"]
            )
        return value

    @pydantic.model_validator(mode="after")
    def _associate_components_with_workspace(self):
        """Associate the components with the workspace"""
        for cmp in (
            self.pipelines
            + self.sinks
            + self.publishers
            + self.scripts
            + self.notebooks
        ):
            cmp._workspace = self
        return self

    @pydantic.field_serializer(
        "pipelines", "sinks", "publishers", "scripts", "notebooks"
    )
    @classmethod
    def _workspace_component_serializer(cls, value: t.Any) -> t.Dict[str, t.Any]:
        """Serialize component arrays back to dictionaries"""
        return {cmp.name: cmp.model_dump() for cmp in value}

    @property
    def path(self) -> Path:
        """Get the path to the workspace"""
        return self.project_path / self.workspace_path

    def __getitem__(self, key: str) -> t.Any:
        """Get a component by name"""
        try:
            if "." in key:
                parts = key.split(".")
                if (
                    parts[0]
                    in ("pipelines", "sinks", "publishers", "scripts", "notebooks")
                    and len(parts) > 1
                ):
                    obj = getattr(self, parts[0])
                    obj = next(filter(lambda cmp: cmp.name == parts[1], obj))
                    parts = parts[2:]
                else:
                    obj = self
                for part in parts:
                    if hasattr(obj, "__getitem__"):
                        obj = obj[part]
                    else:
                        obj = getattr(obj, part)
                return obj
            else:
                return getattr(self, key)
        except AttributeError:
            pass
        raise KeyError(f"Component not found: {key}")

    def __setitem__(self, key: str, value: t.Any) -> None:
        """Set a component by name"""
        raise NotImplementedError("Cannot set components")

    def __delitem__(self, key: str) -> None:
        """Delete a component by name"""
        raise NotImplementedError("Cannot delete components")

    def __len__(self) -> int:
        """Get the number of components"""
        return (
            len(self.pipelines)
            + len(self.sinks)
            + len(self.publishers)
            + len(self.scripts)
            + len(self.notebooks)
        )

    def __iter__(
        self,
    ) -> t.Iterator[spec.CoreSpecification]:
        """Iterate over the components"""
        return iter(
            self.pipelines
            + self.sinks
            + self.publishers
            + self.scripts
            + self.notebooks
        )

    def __contains__(self, key: str) -> bool:
        """Check if a component exists"""
        return key in self.get_component_names()

    def get_component_names(self) -> t.List[str]:
        """Get the component names"""
        return (
            [cmp.name for cmp in self.pipelines]
            + [cmp.name for cmp in self.sinks]
            + [cmp.name for cmp in self.publishers]
            + [cmp.name for cmp in self.scripts]
            + [cmp.name for cmp in self.notebooks]
        )

    keys = get_component_names
    values = __iter__

    def items(self) -> t.Iterator[t.Tuple[str, spec.CoreSpecification]]:
        """Iterate over the components"""
        return itertools.chain(
            zip(self.get_component_names(), self.pipelines),
            zip(self.get_component_names(), self.sinks),
            zip(self.get_component_names(), self.publishers),
            zip(self.get_component_names(), self.scripts),
            zip(self.get_component_names(), self.notebooks),
        )

    def _get_spec(
        self, name: str, kind: str
    ) -> M.Result[spec.CoreSpecification, KeyError]:
        """Get a component spec by name"""
        for cmp in getattr(self, kind):
            if cmp.name == name:
                return M.ok(cmp)
        return M.error(KeyError(f"{kind[:-1].title()} not found: {name}"))

    def get_pipeline_spec(
        self, name: str
    ) -> M.Result[spec.PipelineSpecification, Exception]:
        """Get a pipeline by name"""
        return t.cast(
            M.Result[spec.PipelineSpecification, Exception],
            self._get_spec(name, "pipelines"),
        )

    def get_sink_spec(self, name: str) -> M.Result[spec.SinkSpecification, Exception]:
        """Get a sink by name"""
        return t.cast(
            M.Result[spec.SinkSpecification, Exception],
            self._get_spec(name, "sinks"),
        )

    def get_publisher_spec(
        self, name: str
    ) -> M.Result[spec.PublisherSpecification, Exception]:
        """Get a publisher by name"""
        return t.cast(
            M.Result[spec.PublisherSpecification, Exception],
            self._get_spec(name, "publishers"),
        )

    def get_script_spec(
        self, name: str
    ) -> M.Result[spec.ScriptSpecification, Exception]:
        """Get a script by name"""
        return t.cast(
            M.Result[spec.ScriptSpecification, Exception],
            self._get_spec(name, "scripts"),
        )

    def get_notebook_spec(
        self, name: str
    ) -> M.Result[spec.NotebookSpecification, Exception]:
        """Get a notebook by name"""
        return t.cast(
            M.Result[spec.NotebookSpecification, Exception],
            self._get_spec(name, "notebooks"),
        )

    @property
    def project(self) -> "Project":
        """Get the project this workspace belongs to"""
        if self._project is None:
            raise ValueError("Workspace not associated with a project")
        return self._project

    @property
    def has_project_association(self) -> bool:
        """Check if the workspace is associated with a project"""
        return self._project is not None

    @contextmanager
    def inject_configuration(self) -> t.Iterator[None]:
        """Inject the workspace configuration into the context"""
        with self.project.inject_configuration(self.name):
            yield

    @property
    def fs_adapter(self) -> FilesystemAdapter:
        """Get a handle to the project filesystem adapter"""
        return self.project.fs_adapter

    @property
    def ff_adapter(self) -> AbstractFeatureFlagAdapter:
        """Get a handle to the project feature flag adapter"""
        return self.project.ff_adapter

    def get_transform_gateways(self) -> t.Iterator[t.Tuple[str, "GatewayConfig"]]:
        """Get the SQLMesh gateway configurations"""
        for sink in self.sinks:
            with suppress(KeyError):
                yield sink.name, sink.get_transform_config()

    def get_transform_context(self, name: t.Optional[str] = None):
        """Get the SQLMesh context for the workspace

        We expect a config.py file in the workspace directory that uses the
        `get_transform_gateways` method to populate the SQLMesh Config.gateways key.

        Args:
            name: The name of the gateway to use.

        Returns:
            The SQLMesh context.
        """
        import sqlmesh

        return sqlmesh.Context(paths=self.path, gateway=name)


class Project(_BaseSettings):
    """A project is a collection of workspaces and configuration settings"""

    path: Path = Path(".")
    """The path to the project"""
    name: str = pydantic.Field(
        pattern=r"^[a-zA-Z0-9_\-]+$",
        min_length=3,
        max_length=32,
        default_factory=lambda: "CDF-" + os.urandom(4).hex(sep="-", bytes_per_sep=2),
    )
    """The name of the project"""
    version: str = "0.1.0"
    """The version of the project"""
    owner: t.Optional[str] = None
    """The owner of the project"""
    documentation: t.Optional[str] = None
    """The project documentation"""
    workspaces: t.Tuple[Workspace, ...] = (Workspace(),)
    """The project workspaces"""
    fs: t.Annotated[
        FilesystemConfig,
        pydantic.Field(alias="filesystem"),
    ] = FilesystemConfig()
    """The project filesystem settings"""
    ff: t.Annotated[
        FeatureFlagConfig,
        pydantic.Field(discriminator="provider", alias="feature_flags"),
    ] = FilesystemFeatureFlagConfig()
    """The project feature flags provider settings"""
    state: StateStore = StateStore()
    """The project state connection settings"""

    _wrapped_config: t.Any = {}
    """Store a reference to the wrapped configuration"""

    _extra: t.Dict[str, t.Any] = {}
    """Stored information set via __setitem__ which is included in scoped dictionaries"""

    @pydantic.field_validator("path", mode="before")
    @classmethod
    def _path_validator(cls, value: t.Any):
        """Resolve the project path

        The project path must be a directory. If it is a string, it will be converted to a Path object.
        """
        if isinstance(value, str):
            value = Path(value)
        if not isinstance(value, Path):
            raise ValueError("Path must be a string or a Path object")
        elif not value.is_dir():
            raise FileNotFoundError(f"Project not found: {value}")
        return value.resolve()

    @pydantic.field_validator("workspaces", mode="before")
    @classmethod
    def _workspaces_validator(cls, value: t.Any, info: pydantic.ValidationInfo):
        """Hydrate the workspaces if they are paths. Convert a dict to a list of workspaces.

        If the workspace is a path, load the configuration from the path.
        """
        if isinstance(value, str):
            # pathA; pathB; pathC
            value = list(map(lambda s: s.strip(), value.split(";")))
        elif isinstance(value, dict):
            # name : {config}
            workspaces = []
            for name, config in value.items():
                config.setdefault("name", name)
                workspaces.append(config)
            value = workspaces
        if isinstance(value, (list, tuple)):
            # [{configA} | pathA, {configB}, ...]
            workspaces = []
            project_path = Path(info.data["path"])
            for obj in value:
                if isinstance(obj, (str, Path)):
                    # pathA
                    path = Path(obj)
                    if path.is_absolute():
                        path = path.relative_to(project_path)
                    config = _load_config(project_path / path)
                    config["path"] = path
                    config["project_path"] = project_path
                    workspaces.append(config)
                elif isinstance(obj, dict):
                    # {configA}
                    # NOTE: in the component validator, we have heuristics for getting a path
                    # from a name but we seem to demand a path here, we should be consistent
                    path = Path(obj.pop("path", None) or obj.pop("workspace_path"))
                    if path.is_absolute():
                        path = path.relative_to(project_path)
                    obj["path"] = path
                    obj["project_path"] = project_path
                    workspaces.append(obj)
                else:
                    raise ValueError("Invalid workspace configuration")
            value = workspaces
        if not (hasattr(value, "__iter__") and not isinstance(value, (str, bytes))):
            raise ValueError("Invalid workspaces configuration, must be an iterable")
        return value

    @pydantic.model_validator(mode="after")
    def _project_workspaces_validator(self):
        """Validate the workspaces

        Workspaces must have unique names and paths.
        Workspaces must be subdirectories of the project path.
        Workspaces must not be subdirectories of other workspaces.
        """
        workspace_names = [workspace.name for workspace in self.workspaces]
        if len(workspace_names) != len(set(workspace_names)):
            raise ValueError("Workspace names must be unique")
        workspace_paths = [workspace.path for workspace in self.workspaces]
        if len(workspace_paths) != len(set(workspace_paths)):
            raise ValueError("Workspace paths must be unique")
        if not all(map(lambda path: path.is_relative_to(self.path), workspace_paths)):
            raise ValueError(
                "Workspace paths must be subdirectories of the project path"
            )
        if not all(
            map(
                lambda path: all(
                    not other_path.is_relative_to(path)
                    for other_path in workspace_paths
                    if other_path != path
                ),
                workspace_paths,
            )
        ):
            raise ValueError(
                "Workspace paths must not be subdirectories of other workspaces"
            )
        return self

    @pydantic.model_validator(mode="after")
    def _associate_project_with_children(self):
        """Bind the project to the workspaces, filesystem, and feature flags"""
        for workspace in self.workspaces:
            workspace._project = self
        self.ff._project = self
        self.fs._project = self
        return self

    @pydantic.field_serializer("workspaces")
    @classmethod
    def _workspace_serializer(cls, value: t.Any) -> t.Dict[str, t.Any]:
        """Serialize the workspaces"""
        return {workspace.name: workspace.model_dump() for workspace in value}

    def __getitem__(self, key: str) -> t.Any:
        """Get an item from the configuration"""
        try:
            if "." in key:
                parts = key.split(".")
                if parts[0] == "workspaces" and len(parts) > 1:
                    obj = self.get_workspace(parts[1]).unwrap()
                    parts = parts[2:]
                else:
                    obj = self
                for i, part in enumerate(parts):
                    if isinstance(obj, Workspace):
                        return obj[".".join(parts[i:])]
                    if hasattr(obj, "__getitem__"):
                        obj = obj[part]
                    else:
                        obj = getattr(obj, part)
                return obj
            if key in self.model_fields:
                return getattr(self, key)
        except AttributeError:
            pass
        return self._wrapped_config[key]

    def __setitem__(self, key: str, value: t.Any) -> None:
        """Set an item in the configuration"""
        if key in self.model_fields:
            raise KeyError(
                f"Cannot set `{key}` via string accessor, set the attribute directly instead"
            )
        self._extra[key] = value

    def __delitem__(self, key: str) -> None:
        """Delete a workspace"""
        raise NotImplementedError("Cannot delete workspaces")

    def __len__(self) -> int:
        """Get the number of workspaces"""
        return len(self.workspaces)

    def __iter__(self) -> t.Iterator[Workspace]:
        """Iterate over the workspaces"""
        return iter(self.workspaces)

    def __contains__(self, key: str) -> bool:
        """Check if a workspace exists"""
        return key in self.get_workspace_names()

    def get_workspace_names(self) -> t.List[str]:
        """Get the workspace names"""
        return [workspace.name for workspace in self.workspaces]

    keys = get_workspace_names
    values = __iter__

    def items(self) -> t.Iterator[t.Tuple[str, Workspace]]:
        """Iterate over the workspaces"""
        return zip(self.get_workspace_names(), self.workspaces)

    def get_workspace(self, name: str) -> M.Result[Workspace, Exception]:
        """Get a workspace by name"""
        for workspace in self.workspaces:
            if workspace.name == name:
                return M.ok(workspace)
        return M.error(KeyError(f"Workspace not found: {name}"))

    def get_workspace_from_path(self, path: PathLike) -> M.Result[Workspace, Exception]:
        """Get a workspace by path."""
        path = Path(path).resolve()
        for workspace in self.workspaces:
            if path.is_relative_to(workspace.path):
                return M.ok(workspace)
        return M.error(ValueError(f"No workspace found at {path}."))

    def to_scoped_dict(self, workspace: t.Optional[str] = None) -> ChainMap[str, t.Any]:
        """Convert the project settings to a scoped dictionary

        Lookups are performed in the following order:
        - The extra configuration, holding data set via __setitem__.
        - The workspace configuration, if passed.
        - The project configuration.
        - The wrapped configuration, if available. Typically a dynaconf settings object.

        Boxing allows us to access nested values using dot notation. This is doubly useful
        since ChainMaps will move to the next map in the chain if the dotted key is not
        fully resolved in the current map.
        """

        def to_box(obj: t.Any) -> Box:
            return DynaBox(obj, box_dots=True)

        if workspace:
            return (
                self.get_workspace(workspace)
                .map(
                    lambda ws: ChainMap(
                        to_box(self._extra),
                        to_box(ws.model_dump()),
                        to_box(self.model_dump()),
                        self._wrapped_config,
                    )
                )
                .unwrap()
            )
        return ChainMap(
            to_box(self._extra),
            to_box(self.model_dump()),
            self._wrapped_config,
        )

    @contextmanager
    def inject_configuration(
        self, workspace: t.Optional[str] = None
    ) -> t.Iterator[None]:
        """Inject the project configuration into the context"""
        with inject_configuration(self.to_scoped_dict(workspace)):
            yield

    @cached_property
    def fs_adapter(self) -> FilesystemAdapter:
        """Get a configured filesystem adapter"""
        return self.fs.get_adapter().unwrap()

    @cached_property
    def ff_adapter(self) -> AbstractFeatureFlagAdapter:
        """Get a handle to the project's configured feature flag adapter"""
        return self.ff.get_adapter().unwrap()

    @cached_property
    def duckdb(self) -> duckdb.DuckDBPyConnection:
        """Get a handle to the project's DuckDB connection"""
        conn = duckdb.connect(":memory:")
        conn.install_extension("httpfs")
        conn.install_extension("json")
        conn.register_filesystem(self.fs_adapter.wrapped)
        conn.execute("CREATE TABLE workspaces (name TEXT PRIMARY KEY, path TEXT)")
        for workspace in self.workspaces:
            conn.execute(
                "INSERT INTO workspaces (name, path) VALUES (?, ?)",
                (workspace.name, workspace.path.as_posix()),
            )
        return conn

    def get_workspace_path(self, name: str) -> M.Result[Path, Exception]:
        """Get the path to a workspace by name"""
        return self.get_workspace(name).map(lambda ws: ws.path)

    @classmethod
    def from_path(cls, root: PathLike):
        """Load configuration data from a project root path using dynaconf.

        Args:
            root: The root path to the project.

        Returns:
            A Project object.
        """
        root_path = Path(root).resolve()
        if not root_path.is_dir():
            raise FileNotFoundError(f"Project not found: {root_path}")
        config = _load_config(root_path)
        config["path"] = root_path
        project = cls.model_validate(config)
        project._wrapped_config = config
        return project


def _load_config(
    path: Path, extensions: t.Optional[t.List[str]] = None
) -> dynaconf.LazySettings:
    """Load raw configuration data from a file path using dynaconf.

    Args:
        path: The path to the project or workspace directory

    Returns:
        A dynaconf.LazySettings object.
    """
    extensions = extensions or ["toml", "yaml", "yml", "json", "py"]
    if not any(map(lambda ext: path.joinpath(f"cdf.{ext}").is_file(), extensions)):
        raise FileNotFoundError(f"No cdf configuration file found: {path}")

    config = dynaconf.LazySettings(
        root_path=path,
        settings_files=[f"cdf.{ext}" for ext in extensions],
        environments=True,
        envvar_prefix="CDF",
        env_switcher=c.CDF_ENVIRONMENT,
        env=c.DEFAULT_ENVIRONMENT,
        load_dotenv=True,
        merge_enabled=True,
        validators=[dynaconf.Validator("name", must_exist=True)],
    )

    def _eval_lazy(value: t.Any) -> t.Any:
        """Evaluate lazy values in the configuration"""
        if isinstance(value, dict):
            for key, val in value.items():
                value[key] = _eval_lazy(val)
            return value
        elif isinstance(value, list):
            for i, val in enumerate(value):
                value[i] = _eval_lazy(val)
            return value
        if getattr(value, "_dynaconf_lazy_format", None):
            value = value(config)
        return value

    for key, value in config.items():
        config[key] = _eval_lazy(value)

    return config


load_project = M.result(Project.from_path)
"""Load configuration data from a project root path using dynaconf.

Args:
    root: The root path to the project.

Returns:
    A Result monad with a Project object if successful. Otherwise, a Result monad with an error.
"""

__all__ = [
    "load_project",
    "Project",
    "Workspace",
    "FeatureFlagConfig",
    "FilesystemConfig",
]
