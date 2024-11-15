"""Core classes for managing data packages and projects."""

import importlib.util
import inspect
import sys
import typing as t
from pathlib import Path
from types import ModuleType

from cdf.core.configuration import ConfigBox, ConfigurationLoader
from cdf.core.constants import CONFIG_FILE_NAME, DEFAULT_DATA_PACKAGES_DIR, DEFAULT_DEPENDENCIES_DIR
from cdf.core.container import Container
from cdf.core.extract_load import DltAdapter, ExtractLoadAdapterBase, SingerAdapter, SlingAdapter

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

    def __init__(self, project: "Project", package_path: PathType) -> None:
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
                _load_module_from_path(py_file)
            sys.path.pop(0)

    def _initialize_adapter(self) -> ExtractLoadAdapterBase:
        """Initialize the appropriate extract-load adapter."""
        adapter_type = self.config.get("extract_load_adapter")
        if adapter_type == "dlt":
            return DltAdapter(self)
        elif adapter_type == "sling":
            return SlingAdapter(self)
        elif adapter_type == "singer":
            return SingerAdapter(self)
        else:
            raise ValueError(f"Unsupported extract-load adapter: {adapter_type}")

    @property
    def config(self) -> ConfigBox:
        """Get the data package configuration."""
        return self.container.config

    @property
    def schedules(self) -> t.List[str]:
        """Get defined schedules for the data package."""
        return self.config.get("schedules", [])

    def _load_module(self, module_path: str) -> ModuleType:
        """Load a module from the package directory."""
        sys.path.insert(0, str(self.path))
        try:
            with self.container:
                module = importlib.import_module(module_path)
            return module
        finally:
            sys.path.pop(0)

    def _load_scripts_from_module(self, script_path: Path) -> t.Dict[str, t.Callable]:
        """Load all callable functions from a module."""
        module = self._load_module(script_path.stem)
        functions = {
            name: obj
            for name, obj in inspect.getmembers(module, inspect.isfunction)
            if inspect.getmodule(obj) == module
        }
        return functions

    def discover_extract_load_pipelines(self) -> t.Dict[str, t.Callable]:
        """Delegate to the adapter to discover pipelines."""
        return self.extract_load_adapter.discover_pipelines()

    def run_pipeline(self, pipeline_name: str, **kwargs) -> None:
        """Delegate to the adapter to run the pipeline."""
        with self.container:
            self.extract_load_adapter.run_pipeline(pipeline_name, **kwargs)


class Project(t.Mapping[str, DataPackage]):
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

        self.data_packages: t.Dict[str, DataPackage] = {}
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
        dependencies_dir = self.path / self.container.config.get(
            "dependencies_dir", DEFAULT_DEPENDENCIES_DIR
        )
        if dependencies_dir.exists():
            sys.path.insert(0, str(dependencies_dir))
            for py_file in dependencies_dir.glob("*.py"):
                _load_module_from_path(py_file)
            sys.path.pop(0)

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

    def __iter__(self) -> t.Iterator[str]:
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


class ExtractLoadAdapter(t.Protocol):
    def main(self, **kwargs: t.Any) -> None:
        """Runs the pipeline with the provided arguments."""
        ...


if __name__ == "__main__":
    project = Project("examples/simple_project")
    project.container.add("test1", 123)
    print(project.data_packages)
    print(project.config.some.value)
    print(project.synthetic.discover_extract_load_pipelines())
    project.synthetic.container.add("test2", 321)
    project.synthetic.run_pipeline("main_pipeline")
