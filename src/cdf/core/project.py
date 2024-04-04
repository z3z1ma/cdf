"""A wrapper around a CDF project."""

import os
import typing as t
from collections import ChainMap
from contextlib import suppress
from functools import cached_property
from pathlib import Path

import fsspec
import sqlmesh
from sqlmesh.core.config import GatewayConfig

import cdf.core.logger as logger
from cdf.core.configuration import load_config
from cdf.core.feature_flag import SupportsFFs, load_feature_flag_provider
from cdf.core.filesystem import load_filesystem_provider
from cdf.core.specification import (
    CoreSpecification,
    NotebookSpecification,
    PipelineSpecification,
    PublisherSpecification,
    ScriptSpecification,
    SinkSpecification,
)
from cdf.types import M, PathLike

if t.TYPE_CHECKING:
    import dynaconf

TSpecification = t.TypeVar("TSpecification", bound=CoreSpecification)


class ConfigurationOverlay(ChainMap[str, t.Any]):
    """A ChainMap with attribute access designed to wrap dynaconf settings."""

    def __getattr__(self, name: str) -> t.Any:
        try:
            return self[name]
        except KeyError:
            raise AttributeError(f"No attribute {name}")

    @staticmethod
    def normalize_script(
        config: t.MutableMapping[str, t.Any],
        type_: str,
        ext: t.Tuple[str, ...] = ("py",),
    ) -> t.MutableMapping[str, t.Any]:
        """Normalize a script based configuration.

        The name may be a relative path to the script such as sales/mrr.py in which case the
        path is kept as-is and the name is normalized to sales_mrr.

        Alternatively it could be a name such as mrr in which case the name will be kept as-is
        and the component path will be set to mrr_{type_}.py

        The final example is a name which is a pathlike without an extension such as sales/mrr in which
        case the name will be set to sales_mrr and the path will be set to sales/mrr_{type_}.py

        In the event of multiple extensions for a given script type, and the name ommitting the
        extension, the first extension is used. Any special characters outside os.sep and a file extension
        will cause a pydanitc validation error and prompt the user to update the name property.
        """
        name = config["name"]
        if name.endswith(ext):
            if "path" not in config:
                config["path"] = name
            name = name.rsplit(".", 1)[0]
        else:
            if "path" not in config:
                if name.endswith(f"_{type_}"):
                    config["path"] = f"{name}.{ext[0]}"
                else:
                    config["path"] = f"{name}_{type_}.{ext[0]}"
        config["name"] = name.replace(os.sep, "_")
        return config

    if t.TYPE_CHECKING:
        maps: t.List[dynaconf.Dynaconf]

        def __init__(self, *maps: dynaconf.Dynaconf) -> None: ...


class ContinuousDataFramework:
    """Common properties shared by Project and Workspace."""

    configuration: ConfigurationOverlay

    @property
    def name(self) -> str:
        return self.configuration.name

    @property
    def root(self) -> Path:
        return self.configuration.maps[0]._root_path

    @cached_property
    def feature_flag_provider(self) -> SupportsFFs:
        """The feature flag provider."""
        try:
            ff = self.configuration["feature_flags"]
        except KeyError:
            logger.warning("No feature flag provider configured, defaulting to noop.")
            return load_feature_flag_provider("noop")
        options = ff.setdefault("options", {})
        options.fs = self.filesystem
        return load_feature_flag_provider(ff.provider, options=options.to_dict())

    @cached_property
    def filesystem(self) -> fsspec.AbstractFileSystem:
        """The filesystem provider."""
        try:
            fs = self.configuration["filesystem"]
        except KeyError:
            logger.warning(
                "No filesystem provider configured, defaulting to local with files stored in `_storage`."
            )
            return load_filesystem_provider("file")
        options = fs.setdefault("options", {})
        options.setdefault("auto_mkdir", True)
        return load_filesystem_provider(fs.provider, options=options.to_dict())

    def _get_components_for_spec(
        self,
        spec_cls: t.Type[TSpecification],
        config_key: str,
        script_suffix: t.Optional[str] = None,
    ) -> t.Dict[str, TSpecification]:
        """Get components for a specification.

        Args:
            spec_cls: The specification class.
            config_key: The configuration key.
            script_suffix: The default suffix for the scripts. If None, the config_key is used with
                the last character removed.
        """
        components = {}
        if config_key not in self.configuration:
            return components
        for key, config in self.configuration[config_key].items():
            config.setdefault("name", key)
            config["workspace_path"] = self.root
            component = spec_cls.model_validate(
                self.configuration.normalize_script(
                    config,
                    script_suffix or config_key[:-1],
                    ("ipynb", "py") if spec_cls is NotebookSpecification else ("py",),
                ),
                from_attributes=True,
            )
            components[component.name] = component
        return components

    @cached_property
    def pipelines(self) -> t.Dict[str, PipelineSpecification]:
        """Map of pipelines by name."""
        return self._get_components_for_spec(PipelineSpecification, "pipelines")

    @cached_property
    def sinks(self) -> t.Dict[str, SinkSpecification]:
        """Map of sinks by name."""
        return self._get_components_for_spec(SinkSpecification, "sinks")

    @cached_property
    def publishers(self) -> t.Dict[str, PublisherSpecification]:
        """Map of publishers by name."""
        return self._get_components_for_spec(PublisherSpecification, "publishers")

    @cached_property
    def scripts(self) -> t.Dict[str, ScriptSpecification]:
        """Map of scripts by name."""
        return self._get_components_for_spec(ScriptSpecification, "scripts")

    @cached_property
    def notebooks(self) -> t.Dict[str, NotebookSpecification]:
        """Map of notebooks by name."""
        return self._get_components_for_spec(NotebookSpecification, "notebooks")

    def get_pipeline(self, name: str) -> M.Result[PipelineSpecification, Exception]:
        """Get a pipeline by name."""
        try:
            return M.ok(self.pipelines[name])
        except Exception as e:
            return M.error(e)

    def get_sink(self, name: str) -> M.Result[SinkSpecification, Exception]:
        """Get a sink by name."""
        try:
            return M.ok(self.sinks[name])
        except Exception as e:
            return M.error(e)

    def get_publisher(self, name: str) -> M.Result[PublisherSpecification, Exception]:
        """Get a publisher by name."""
        try:
            return M.ok(self.publishers[name])
        except Exception as e:
            return M.error(e)

    def get_script(self, name: str) -> M.Result[ScriptSpecification, Exception]:
        """Get a script by name."""
        try:
            return M.ok(self.scripts[name])
        except Exception as e:
            return M.error(e)

    def get_notebook(self, name: str) -> M.Result[NotebookSpecification, Exception]:
        """Get a notebook by name."""
        try:
            return M.ok(self.notebooks[name])
        except Exception as e:
            return M.error(e)

    def get_gateways(self) -> M.Result[t.Dict[str, GatewayConfig], Exception]:
        """Convert the project's gateways to a dictionary."""
        gateways = {}
        for sink in self.sinks.values():
            with suppress(KeyError):
                gateways[sink.name] = sink.get_transform_config()
        if not gateways:
            return M.error(ValueError(f"No gateways in workspace {self.name}"))
        return M.ok(gateways)

    def get_transform_context(self, sink: str) -> sqlmesh.Context:
        """Get a transform context for a sink."""
        return sqlmesh.Context(paths=self.root, gateway=sink)

    def __getitem__(self, key: str) -> t.Any:
        return self.configuration[key]

    def __setitem__(self, key: str, value: t.Any) -> None:
        self.configuration[key] = value


class Project(ContinuousDataFramework):
    """A CDF project."""

    def __init__(
        self,
        configuration: "dynaconf.Dynaconf",
        workspaces: t.Dict[str, "dynaconf.Dynaconf"],
    ) -> None:
        """Initialize a project."""
        self.configuration = ConfigurationOverlay(configuration)
        self._workspaces = workspaces

    def get_workspace(self, name: str) -> M.Result["Workspace", Exception]:
        """Get a workspace by name."""
        try:
            return M.ok(Workspace(name, project=self))
        except Exception as e:
            return M.error(e)

    def get_workspace_from_path(
        self, path: PathLike
    ) -> M.Result["Workspace", Exception]:
        """Get a workspace by path."""
        path = Path(path).resolve()
        for name, workspace in self._workspaces.items():
            if path.is_relative_to(workspace._root_path):
                return self.get_workspace(name)
        return M.error(ValueError(f"No workspace found at {path}."))

    @classmethod
    def load(cls, root: PathLike) -> "Project":
        """Create a project from a root path."""
        config = load_config(root).unwrap()
        return cls(config["project"], workspaces=config["workspaces"])


class Workspace(ContinuousDataFramework):
    """A CDF workspace."""

    def __init__(self, name: str, /, *, project: Project) -> None:
        """Initialize a workspace."""
        self._project = project
        self.configuration = ConfigurationOverlay(
            project._workspaces[name],
            project.configuration.maps[0],
        )

    @property
    def parent(self) -> Project:
        """The parent project."""
        return self._project


load_project = M.result(Project.load)
"""Create a project from a root path."""
