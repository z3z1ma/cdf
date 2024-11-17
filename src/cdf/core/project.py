# pyright: reportUnknownVariableType=false, reportUnknownMemberType=false, reportUnknownArgumentType=false
"""Core classes for managing data packages and projects."""

from __future__ import annotations

import sys
import typing as t
from collections.abc import Iterator, Mapping
from pathlib import Path

from cdf.core.configuration import ConfigBox, ConfigurationLoader
from cdf.core.constants import CONFIG_FILE_NAME, DEFAULT_DATA_PACKAGES_DIR, DEFAULT_DEPENDENCIES_DIR
from cdf.core.container import Container
from cdf.core.extract_load import DltAdapter, ExtractLoadAdapterBase, SingerAdapter, SlingAdapter
from cdf.utils.file import load_module_from_path

PathType = Path | str


@t.final
class DataPackage:
    """Represents a data package with its own container and processing logic."""

    def __init__(self, project: Project, package_path: PathType) -> None:
        """Initialize the data package.

        Args:
            project: The project containing the data package.
            package_path: Path to the data package directory.
        """
        self.project = project
        self.path = Path(package_path)
        self.name = self.path.name
        self.container = self._create_container()
        self._load_dependencies()
        self.extract_load_adapter = self._initialize_adapter()

    def _create_container(self) -> Container:
        """Create a container for the data package, inheriting from the parent container."""
        return Container(
            config=ConfigurationLoader.from_name(
                CONFIG_FILE_NAME,
                search_paths=[
                    self.project.path,
                    self.path,
                    Path.home() / ".cdf",
                ],
            ).load(),
            namespace=self.name,
            parent=self.project.container,
        )

    def _load_dependencies(self) -> None:
        """Load dependencies from Python files in the 'dependencies' directory."""
        dependencies_dir = self.path / "dependencies"
        if dependencies_dir.exists():
            sys.path.insert(0, str(dependencies_dir))
            for py_file in dependencies_dir.glob("*.py"):
                _ = load_module_from_path(py_file)
            _ = sys.path.pop(0)

    # TODO: next step is probably to refactor this?
    # runtime polymorphism is good actually, but lets be sure on the interface
    def _initialize_adapter(self) -> ExtractLoadAdapterBase:
        """Initialize the appropriate extract-load adapter."""
        adapter_type = self.config.get("extract_load_adapter")
        if not isinstance(adapter_type, str):
            raise TypeError("Extract-load adapter must be a string")
        if adapter_type == "dlt":
            adapter_impl = DltAdapter
        elif adapter_type == "sling":
            adapter_impl = SlingAdapter
        elif adapter_type == "singer":
            adapter_impl = SingerAdapter
        else:
            raise ValueError(f"Unsupported extract-load adapter: {adapter_type}")
        return adapter_impl(self.path, self.config)

    @property
    def config(self) -> ConfigBox:
        """Get the data package configuration."""
        return self.container.config

    @property
    def schedules(self) -> list[str]:
        """Get defined schedules for the data package."""
        schedules = self.config.get("schedules", [])
        if not isinstance(schedules, list):
            raise TypeError("Schedules must be a list")
        return schedules

    def discover_extract_load_pipelines(self) -> dict[str, t.Callable[..., t.Any]]:
        """Delegate to the adapter to discover pipelines."""
        return self.extract_load_adapter.discover_pipelines()

    def run_pipeline(self, pipeline_name: str, **kwargs: t.Any) -> None:
        """Delegate to the adapter to run the pipeline."""
        with self.container:
            self.extract_load_adapter.run_pipeline(pipeline_name, **kwargs)


@t.final
class Project(Mapping[str, DataPackage]):
    """Manages a project with its data packages and container."""

    def __init__(self, project_path: PathType) -> None:
        """Initialize the project.

        Args:
            project_path: Path to the project directory.
        """
        self.path = Path(project_path)
        self.name = self.path.name

        self.container = self._create_container()
        self._load_dependencies()

        self.data_packages: dict[str, DataPackage] = {}
        self._discover_data_packages()

    def _create_container(self) -> Container:
        """Create a container for the project."""
        return Container(
            config=ConfigurationLoader.from_name(
                CONFIG_FILE_NAME,
                search_paths=[
                    self.path,
                    Path.home() / ".cdf",
                ],
            ).load(),
            namespace=self.name,
        )

    def _load_dependencies(self) -> None:
        """Load dependencies from Python files in the 'dependencies' directory."""
        dependencies_dir = self.path / str(
            self.container.config.get("dependencies_dir", DEFAULT_DEPENDENCIES_DIR)
        )
        if dependencies_dir.exists():
            sys.path.insert(0, str(dependencies_dir))
            for py_file in dependencies_dir.glob("*.py"):
                _ = load_module_from_path(py_file)
            _ = sys.path.pop(0)

    def _discover_data_packages(self) -> None:
        """Discover and load data packages within the project."""
        data_packages_dir = self.path / self.container.config.get(
            "data_packages_dir", DEFAULT_DATA_PACKAGES_DIR
        )
        if data_packages_dir.exists():
            for package_dir in data_packages_dir.iterdir():
                if package_dir.is_dir():
                    data_package = DataPackage(self, package_dir)
                    self.data_packages[data_package.name] = data_package

    @property
    def config(self) -> ConfigBox:
        """Get the project configuration."""
        return self.container.config

    def __getitem__(self, key: str) -> DataPackage:
        return self.data_packages[key]

    def __iter__(self) -> Iterator[str]:
        return iter(self.data_packages)

    def __len__(self) -> int:
        return len(self.data_packages)

    def __getattr__(self, key: str) -> DataPackage:
        try:
            return self[key]
        except KeyError as e:
            raise AttributeError(f"No data package found with name: {key}") from e

    def __repr__(self) -> str:
        return f"Project({self.path})"


if __name__ == "__main__":
    project = Project("../cdf-toy-project")
    print(project)
    project.container.add("test1", 123)
    print(project.data_packages)
    print(project.config.some.value)
    print(project.synthetic.discover_extract_load_pipelines())
    print(project.synthetic.extract_load_adapter)
    project.synthetic.container.add("test2", 321)
    project.synthetic.run_pipeline("pipeline_main")
