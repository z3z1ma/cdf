# pyright: reportUnknownVariableType=false, reportUnknownMemberType=false, reportUnknownArgumentType=false
"""Core classes for managing data packages and projects."""

from __future__ import annotations

import typing as t
from collections.abc import Iterator, Mapping
from functools import wraps
from pathlib import Path
from types import ModuleType

import cdf.core.adapter as A
import cdf.core.interface as I
from cdf.commons.file import load_module_from_path
from cdf.commons.pyutils import inject_sys_path
from cdf.core.configuration import ConfigurationLoader
from cdf.core.constants import CONFIG_FILE_NAME
from cdf.core.container import Container

__all__ = ["DataPackage", "Project"]

T = t.TypeVar("T")
P = t.ParamSpec("P")
PathType = Path | str


def inject_package(func: t.Callable[P, T]) -> t.Callable[P, T]:
    """A decorator to run a function with a container context."""

    @wraps(func)
    def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
        self = t.cast(Project | DataPackage, args[0])
        with self.container, inject_sys_path(str(self.path)):
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
        self.settings = I.DataPackageConfig.model_validate(
            self.container.cfg.package, from_attributes=True
        )
        self.container["cdf_package"] = self

        self._dependencies = self._load_dependencies()

        if self.settings.extract_load:
            self._extract_load_adapter = A.extract_load_adapter_factory(
                self.path, self.container, conf=self.settings.extract_load
            )
        else:
            self._extract_load_adapter = None

        if self.settings.test:
            self._test_adapter = A.test_adapter_factory(self.path, self.settings.test)
        else:
            self._test_adapter = None

        if self.settings.transform:
            self._transform_adapter = A.transform_adapter_factory(
                self.path, self.settings.transform
            )
        else:
            self._transform_adapter = None

        self.state = project.state.scope(self.name)
        self.container.add("cdf_state", self.state)

    def _create_container(self) -> Container:
        """Create a container for the data package, inheriting from the parent container."""
        container = Container(
            config=ConfigurationLoader.from_name(
                CONFIG_FILE_NAME,
                search_paths=[
                    self.project.path,
                    self.path / "pyproject.toml",
                    self.path,
                    Path.home() / ".cdf",
                ],
                context="package",
            ),
            namespace=self.name,
            parent=self.project.container,
        )
        return container

    def _load_dependencies(self) -> tuple[ModuleType, ...]:
        """Load dependencies from Python files in the 'dependencies' directory."""
        dependencies_dir = self.path / self.project.settings.dependencies_dir
        if dependencies_dir.exists():
            with self.container, inject_sys_path(dependencies_dir):
                return tuple(
                    load_module_from_path(py_file) for py_file in dependencies_dir.glob("*.py")
                )
        return ()

    def activate(self) -> None:
        """Set the data package container as the active container."""
        _ = self.container.activate()

    @property
    def extract_load_adapter(self) -> A.extract_load.ExtractLoadAdapterBase[t.Any]:
        if self._extract_load_adapter is None:
            raise ValueError(f"No extract-load adapter configured for the {self.name} package")
        return self._extract_load_adapter

    @property
    def test_adapter(self) -> A.test.TestAdapterBase[t.Any]:
        if self._test_adapter is None:
            raise ValueError(f"No test adapter configured for the {self.name} package")
        return self._test_adapter

    @property
    def transform_adapter(self) -> A.transform.TransformationAdapterBase:
        if self._transform_adapter is None:
            raise ValueError(f"No transformation adapter configured for the {self.name} package")
        return self._transform_adapter

    @inject_package
    def discover_extract_load_pipelines(self) -> Mapping[str, t.Callable[..., t.Any]]:
        """Delegate to the adapter to discover pipelines."""
        return self.extract_load_adapter.discover_pipelines()

    @inject_package
    def run_pipeline(self, pipeline_name: str, /, **kwargs: t.Any) -> None:
        """Delegate to the adapter to run the pipeline."""
        self.extract_load_adapter(pipeline_name, **kwargs)

    @inject_package
    def run_tests(self) -> Mapping[str, t.Any]:
        """Run tests using the test adapter."""
        results, err = self.test_adapter()
        if err:
            raise AssertionError(f"Tests failed for package {self.name}:\n{results}")
        return results

    @inject_package
    def run_transformations(self, **kwargs: t.Any) -> None:
        """Run transformations using the transformation adapter."""
        self.transform_adapter(**kwargs)


@t.final
class Project(Mapping[str, DataPackage]):
    """Manages a project with its data packages and container."""

    def __init__(self, project_path: PathType) -> None:
        """Initialize the project.

        Args:
            project_path: Path to the project directory.
        """
        self.path = Path(project_path)
        if not self.path.exists():
            raise FileNotFoundError(f"Project path '{self.path}' does not exist.")

        self.container = self._create_container()
        if "project" not in self.container.cfg:
            raise ValueError(
                f"No project configuration found in {self.path}, ensure a project key is defined."
            )
        self.settings = I.ProjectConfig.model_validate(
            self.container.cfg.project, from_attributes=True
        )
        self.container["cdf_project"] = self

        self._load_dependencies()

        if self.settings.state_backend:
            self.state = A.state_backend_factory(self.path, self.settings.state_backend)
        else:
            self.state = A.get_default_file_state_backend(self.path)
        self.container.add("cdf_state", self.state)

        self.data_packages: dict[str, DataPackage] = {}
        self._discover_data_packages()

    @property
    def name(self) -> str:
        return self.settings.name

    def _create_container(self) -> Container:
        """Create a container for the project."""
        container = Container(
            config=ConfigurationLoader.from_name(
                CONFIG_FILE_NAME,
                search_paths=[
                    self.path / "pyproject.toml",
                    self.path,
                    Path.home() / ".cdf",
                ],
                context="project",
            ),
            namespace="__main__",
        )
        return container

    def _load_dependencies(self) -> None:
        """Load dependencies from Python files in the 'dependencies' directory."""
        dependencies_dir = self.path / self.settings.dependencies_dir
        if dependencies_dir.exists():
            with self.container, inject_sys_path(dependencies_dir):
                for py_file in dependencies_dir.glob("*.py"):
                    _ = load_module_from_path(py_file)

    def _discover_data_packages(self) -> None:
        """Discover and load data packages within the project."""
        data_packages_dir = self.path / self.settings.data_packages_dir
        if data_packages_dir.exists():
            for package_dir in data_packages_dir.iterdir():
                if package_dir.is_dir():
                    data_package = DataPackage(self, package_dir)
                    self.data_packages[data_package.name] = data_package

    def __getitem__(self, key: str) -> DataPackage:
        return self.data_packages[key]

    def __iter__(self) -> Iterator[str]:
        return iter(self.data_packages)

    def __len__(self) -> int:
        return len(self.data_packages)

    def __getattr__(self, key: str) -> DataPackage:
        try:
            return self.data_packages[key]
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
    print("project.config.some.value", project.container.cfg.some.value)

    print("Discovered pipelines", project.synthetic.discover_extract_load_pipelines())
    print("Index into pipeline", project.synthetic.extract_load_adapter.pipeline_main)

    print("Adding `test2` to project container")
    project.synthetic.container.add("test2", 321)

    print("Running pipeline `pipeline_main`")
    project.synthetic.run_pipeline("pipeline_main")

    print("Running tests for `synthetic` package")
    _ = project.synthetic.run_tests()
