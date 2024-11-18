# pyright: reportUnknownVariableType=false, reportUnknownMemberType=false, reportUnknownArgumentType=false
"""Core classes for managing data packages and projects."""

from __future__ import annotations

import sys
import typing as t
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from functools import wraps
from pathlib import Path
from types import ModuleType

from cdf.core.configuration import ConfigBox, ConfigurationLoader
from cdf.core.constants import CONFIG_FILE_NAME, DEFAULT_DATA_PACKAGES_DIR, DEFAULT_DEPENDENCIES_DIR
from cdf.core.container import Container
from cdf.core.extract_load import DltAdapter, ExtractLoadAdapterBase, SingerAdapter, SlingAdapter
from cdf.core.testing import DbtTestAdapter, PytestAdapter, TestAdapterBase, UnittestAdapter
from cdf.utils.file import load_module_from_path

__all__ = ["DataPackage", "Project"]

T = t.TypeVar("T")
P = t.ParamSpec("P")
PathType = Path | str


@contextmanager
def _inject_sys_path(*paths: str) -> Iterator[None]:
    """Temporarily add paths to sys.path.

    Args:
        paths (List[str]): List of paths to temporarily add to sys.path.

    Yields:
        None
    """
    original_sys_path = sys.path[:]
    try:
        sys.path[:0] = paths
        yield
    finally:
        sys.path = original_sys_path


def run_with_context(func: t.Callable[P, T]) -> t.Callable[P, T]:
    """A decorator to run a function with a container context."""

    @wraps(func)
    def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
        self = t.cast(Project | DataPackage, args[0])
        with self.container, _inject_sys_path(str(self.path)):
            return func(*args, **kwargs)

    return wrapper


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
        self._dependencies = self._load_dependencies()
        self._extract_load_adapter = self._initialize_el_adapter()
        self._test_adapter = self._initialize_test_adapter()

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

    def _load_dependencies(self) -> tuple[ModuleType, ...]:
        """Load dependencies from Python files in the 'dependencies' directory."""
        dependencies_dir = self.path / "dependencies"
        if dependencies_dir.exists():
            sys.path.insert(0, str(dependencies_dir))
            try:
                return tuple(
                    load_module_from_path(py_file) for py_file in dependencies_dir.glob("*.py")
                )
            finally:
                _ = sys.path.pop(0)
        return ()

    def _initialize_el_adapter(self) -> ExtractLoadAdapterBase:
        """Initialize the appropriate extract-load adapter."""
        match self.config.get("extract_load_adapter"):
            case "dlt":
                adapter_impl = DltAdapter
            case "sling":
                adapter_impl = SlingAdapter
            case "singer":
                adapter_impl = SingerAdapter
            case _:
                raise ValueError("Unsupported extract-load adapter")
        return adapter_impl(self.path, self.config)

    def _initialize_test_adapter(self) -> TestAdapterBase[t.Any]:
        """Initialize the test adapter."""
        match self.config.get("test_adapter", "pytest"):
            case "pytest":
                adapter_impl = PytestAdapter
            case "unittest":
                adapter_impl = UnittestAdapter
            case "dbt":
                adapter_impl = DbtTestAdapter
            case _:
                raise ValueError("Unsupported test adapter")
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

    @run_with_context
    def discover_extract_load_pipelines(self) -> dict[str, t.Callable[..., t.Any]]:
        """Delegate to the adapter to discover pipelines."""
        return self._extract_load_adapter.discover_pipelines()

    @run_with_context
    def run_pipeline(self, pipeline_name: str, **kwargs: t.Any) -> None:
        """Delegate to the adapter to run the pipeline."""
        self._extract_load_adapter(pipeline_name, **kwargs)

    @run_with_context
    def run_tests(self) -> Mapping[str, t.Any]:
        """Run tests using the test adapter."""
        success, results = self._test_adapter()
        if not success:
            raise AssertionError(f"Tests failed for package {self.name}:\n{results}")
        return results


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

    print("Project", project)

    print("Adding `test1` to project container")
    project.container.add("test1", 123)

    print("project.data_packages", project.data_packages)
    print("project.config.some.value", project.config.some.value)

    print("Discovered pipelines", project.synthetic.discover_extract_load_pipelines())
    print("Index into pipeline", project.synthetic._extract_load_adapter.pipeline_main)  # pyright: ignore[reportPrivateUsage]

    print("Adding `test2` to project container")
    project.synthetic.container.add("test2", 321)

    print("Running pipeline `pipeline_main`")
    project.synthetic.run_pipeline("pipeline_main")

    print("Running tests for `synthetic` package")
    _ = project.synthetic.run_tests()
