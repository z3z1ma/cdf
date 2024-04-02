"""A wrapper around a CDF project."""

import typing as t
from collections import ChainMap
from functools import cached_property
from pathlib import Path

import fsspec

from cdf.core.configuration import load_config
from cdf.core.feature_flag import SupportsFFs, load_feature_flag_provider
from cdf.core.filesystem import load_filesystem_provider
from cdf.core.specification import PipelineSpecification
from cdf.types import M, PathLike

if t.TYPE_CHECKING:
    import dynaconf


class ConfigurationOverlay(ChainMap[str, t.Any]):
    """A ChainMap with attribute access designed to wrap dynaconf settings."""

    def __getattr__(self, name: str) -> t.Any:
        try:
            return self[name]
        except KeyError:
            raise AttributeError(f"No attribute {name}")

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
        ff = self.configuration["feature_flags"]
        return load_feature_flag_provider(ff.provider, options=ff.options.to_dict())

    @cached_property
    def filesystem(self) -> fsspec.AbstractFileSystem:
        """The filesystem provider."""
        fs = self.configuration["filesystem"]
        return load_filesystem_provider(fs.provider, options=fs.options.to_dict())

    # TODO: we just need to pass the filepath here?
    # well previously we generated the Config object inside a user defined sink function...
    @cached_property
    def sqlmesh_context(self) -> t.Any: ...

    def get_pipeline(self, name: str) -> M.Result[PipelineSpecification, Exception]:
        """Get a pipeline by name."""
        try:
            return M.ok(
                PipelineSpecification.from_config(
                    name,
                    root=self.root,
                    config=self.configuration["pipelines"][name],
                )
            )
        except Exception as e:
            return M.error(e)

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


@M.result
def get_project(root: PathLike) -> Project:
    """Create a project from a root path."""
    config = load_config(root).unwrap()
    config["root"]
    return Project(config["project"], workspaces=config["workspaces"])
