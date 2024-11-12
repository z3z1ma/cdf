"""Core classes for managing data packages and projects."""

import collections
import importlib.util
import sys
import typing as t
from pathlib import Path

from cdf.core.configuration import ConverterBox, SimpleConfigurationLoader
from cdf.core.constants import (
    CONFIG_FILE_NAME,
    DEFAULT_DATA_PACKAGES_DIR,
    DEFAULT_DEPENDENCIES_DIR,
)
from cdf.core.context import Context

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
    """Represents a data package with its own context and processing logic."""

    def __init__(
        self,
        package_path: PathType,
        parent_context: Context,
    ) -> None:
        """Initialize the data package.

        Args:
            package_path: Path to the data package directory.
            parent_context: The parent context from the project.
        """
        self.package_path = Path(package_path)
        self.name = self.package_path.name
        self.parent_context = parent_context

        self.context = self._create_context()
        self._load_dependencies()

    def _create_context(self) -> Context:
        """Create a context for the data package, inheriting from the parent context."""
        return Context(
            loader=SimpleConfigurationLoader.from_name(
                CONFIG_FILE_NAME, search_path=self.package_path
            ),
            namespace=self.name,
            parent=self.parent_context,
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
    def config(self) -> t.Dict[str, t.Any]:
        """Get the configuration for the data package."""
        return ConverterBox(
            collections.ChainMap(self.context.config, self.parent_context.config),
            box_dots=True,
        )


class Project:
    """Manages a project with its data packages and context."""

    def __init__(self, project_path: PathType) -> None:
        """Initialize the project.

        Args:
            project_path: Path to the project directory.
        """
        self.project_path = Path(project_path)
        self.name = self.project_path.name

        self.context = self._create_context()
        self._load_dependencies()

        self.data_packages: t.Dict[str, DataPackage] = {}
        self._discover_data_packages()

    def _create_context(self) -> Context:
        """Create a context for the project."""
        return Context(
            loader=SimpleConfigurationLoader.from_name(
                CONFIG_FILE_NAME, search_path=self.project_path
            ),
            namespace=self.name,
        )

    def _load_dependencies(self) -> None:
        """Load dependencies from Python files in the 'dependencies' directory."""
        dependencies_dir = self.project_path / DEFAULT_DEPENDENCIES_DIR
        if dependencies_dir.exists():
            sys.path.insert(0, str(dependencies_dir))
            for py_file in dependencies_dir.glob("*.py"):
                _load_module_from_path(py_file)
            sys.path.pop(0)

    def _discover_data_packages(self) -> None:
        """Discover and load data packages within the project."""
        data_packages_dir = self.context.config.get(
            "data_packages_dir", DEFAULT_DATA_PACKAGES_DIR
        )
        packages_dir = self.project_path / data_packages_dir
        if packages_dir.exists():
            for package_dir in packages_dir.iterdir():
                if package_dir.is_dir():
                    data_package = DataPackage(package_dir, parent_context=self.context)
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
    def config(self) -> t.Dict[str, t.Any]:
        """Get the top-level configuration for the project."""
        return self.context.config


if __name__ == "__main__":
    project = Project(".")
    print(project.data_packages)
    print(project.context.config["z"])
    print(project.data_packages["synthetic"].config["z"])
