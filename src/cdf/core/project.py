"""Core classes for managing data packages and projects."""

import importlib.util
import sys
import typing as t
from pathlib import Path


from cdf.core.configuration import ConfigurationLoader, ConfigBox
from cdf.core.constants import (
    CONFIG_FILE_NAME,
    DEFAULT_DATA_PACKAGES_DIR,
    DEFAULT_DEPENDENCIES_DIR,
)
from cdf.core.container import Container

PathType = t.Union[str, Path]


def _load_module_from_path(path: Path) -> t.Dict[str, t.Any]:
    """Load a Python module from a file path."""
    module_name = path.stem
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Could not load module from path: {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.__dict__


class DataPackage:
    """Represents a data package with its own container and processing logic."""

    def __init__(
        self,
        package_path: PathType,
        parent_container: Container,
    ) -> None:
        """Initialize the data package.

        Args:
            package_path: Path to the data package directory.
            parent_container: The parent container from the project.
        """
        self.package_path = Path(package_path)
        self.name = self.package_path.name
        self.parent_container = parent_container

        self.container = self._create_container()
        self._load_dependencies()

    def _create_container(self) -> Container:
        """Create a container for the data package, inheriting from the parent container."""
        return Container(
            config=ConfigurationLoader.from_name(
                CONFIG_FILE_NAME, search_path=self.package_path
            ).load(),
            namespace=self.name,
            parent=self.parent_container,
        )

    def _load_dependencies(self) -> None:
        """Load dependencies from Python files in the 'dependencies' directory."""
        dependencies_dir = self.package_path / "dependencies"
        if dependencies_dir.exists():
            sys.path.insert(0, str(dependencies_dir))
            for py_file in dependencies_dir.glob("*.py"):
                _load_module_from_path(py_file)
            sys.path.pop(0)

    @property
    def config(self) -> ConfigBox:
        """Get the data package configuration."""
        return self.container.config


class Project:
    """Manages a project with its data packages and container."""

    def __init__(self, project_path: PathType) -> None:
        """Initialize the project.

        Args:
            project_path: Path to the project directory.
        """
        self.project_path = Path(project_path)
        self.name = self.project_path.name

        self.container = self._create_container()
        self._load_dependencies()

        self.data_packages: t.Dict[str, DataPackage] = {}
        self._discover_data_packages()

    def _create_container(self) -> Container:
        """Create a container for the project."""
        return Container(
            config=ConfigurationLoader.from_name(
                CONFIG_FILE_NAME, search_path=self.project_path
            ).load(),
            namespace=self.name,
        )

    def _load_dependencies(self) -> None:
        """Load dependencies from Python files in the 'dependencies' directory."""
        dependencies_dir = self.project_path / self.container.config.get(
            "dependencies_dir", DEFAULT_DEPENDENCIES_DIR
        )
        if dependencies_dir.exists():
            sys.path.insert(0, str(dependencies_dir))
            for py_file in dependencies_dir.glob("*.py"):
                _load_module_from_path(py_file)
            sys.path.pop(0)

    def _discover_data_packages(self) -> None:
        """Discover and load data packages within the project."""
        data_packages_dir = self.project_path / self.container.config.get(
            "data_packages_dir", DEFAULT_DATA_PACKAGES_DIR
        )
        if data_packages_dir.exists():
            for package_dir in data_packages_dir.iterdir():
                if package_dir.is_dir():
                    data_package = DataPackage(
                        package_dir, parent_container=self.container
                    )
                    self.data_packages[data_package.name] = data_package

    def get_data_package(self, name: str) -> t.Optional[DataPackage]:
        """Get a data package by name.

        Args:
            name: Name of the data package.

        Returns:
            The DataPackage instance if found, else None.
        """
        return self.data_packages.get(name)

    @property
    def config(self) -> ConfigBox:
        """Get the project configuration."""
        return self.container.config


if __name__ == "__main__":
    project = Project(".")
    print(project.data_packages)
    print(project.config.some.value)
    print(project.data_packages["synthetic"].config.other.value)
