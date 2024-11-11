from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import tomli
import yaml

from .context import Context, SimpleConfigurationLoader

# Type definitions
Configuration = Dict[str, Any]
PathType = Union[str, Path]


class DataPackage:
    """Represents a data package with its own context, configurations, and processing logic."""

    def __init__(self, package_path: PathType, parent_context: Context) -> None:
        """Initialize the data package.

        Args:
            package_path: Path to the data package directory.
            parent_context: The parent context from the project.
        """
        self.package_path = Path(package_path)
        self.name = self.package_path.name
        self.parent_context = parent_context
        self.context = self._create_context()
        self.config = self.context.config

        # Load additional configurations or settings as needed
        self._load_pyproject()
        self._load_metadata()

    def _create_context(self) -> Context:
        """Create a context for the data package, inheriting from the parent context."""
        # Load configurations from package-specific sources
        loader = SimpleConfigurationLoader(
            self.package_path / "config.yaml", include_env=True
        )
        return Context(loader=loader, namespace=self.name, parent=self.parent_context)

    def _load_pyproject(self) -> None:
        """Load the pyproject.toml file to get package requirements and settings."""
        pyproject_path = self.package_path / "pyproject.toml"
        if pyproject_path.exists():
            with open(pyproject_path, "rb") as f:
                self.pyproject = tomli.load(f)
            # Process the pyproject content as needed
        else:
            self.pyproject = {}

    def _load_metadata(self) -> None:
        """Load metadata such as datasets produced and PII indicators."""
        metadata_path = self.package_path / "metadata.yaml"
        if metadata_path.exists():
            with open(metadata_path, "r") as f:
                self.metadata = yaml.safe_load(f)
            # Process metadata as needed
        else:
            self.metadata = {}

    def get_datasets(self) -> List[str]:
        """Return the list of datasets produced by this data package."""
        return self.metadata.get("datasets", [])

    def setup_extraction(self) -> None:
        """Set up extraction using dlt or other specified tools."""
        # Implement extraction setup logic, focusing on dlt
        pass

    def setup_transformation(self) -> None:
        """Set up transformations using sqlmesh or other specified tools."""
        # Implement transformation setup logic, focusing on sqlmesh
        pass

    def run(self) -> None:
        """Run the data package processing."""
        self.setup_extraction()
        self.setup_transformation()
        # Implement the processing logic
        pass


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
        self.config = self.context.config
        self.data_packages: Dict[str, DataPackage] = {}
        self._discover_data_packages()

    def _create_context(self) -> Context:
        """Create a context for the project."""
        # Load configurations from project-specific sources
        loader = SimpleConfigurationLoader(
            self.project_path / "config.yaml", include_env=True
        )
        return Context(loader=loader, namespace=self.name)

    def _discover_data_packages(self) -> None:
        """Discover and load data packages within the project."""
        packages_dir = self.project_path / "data_packages"
        if packages_dir.exists():
            for package_dir in packages_dir.iterdir():
                if package_dir.is_dir():
                    data_package = DataPackage(package_dir, parent_context=self.context)
                    self.data_packages[data_package.name] = data_package

    def get_data_package(self, name: str) -> Optional[DataPackage]:
        """Get a data package by name.

        Args:
            name: Name of the data package.

        Returns:
            The DataPackage instance if found, else None.
        """
        return self.data_packages.get(name)

    def run(self) -> None:
        """Run all data packages in the project."""
        for data_package in self.data_packages.values():
            data_package.run()


# Example usage
if __name__ == "__main__":
    project = Project("/path/to/project")
    project.run()
