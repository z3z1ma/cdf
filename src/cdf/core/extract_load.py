import subprocess  # nosec
import typing as t
from abc import ABC, abstractmethod

if t.TYPE_CHECKING:
    from cdf.core.project import DataPackage


class ExtractLoadAdapterBase(ABC):
    """Abstract base class for all extract-load adapters."""

    def __init__(self, package: "DataPackage") -> None:
        self.package = package

    @abstractmethod
    def discover_pipelines(self) -> t.Dict[str, t.Callable]:
        """Discover available pipelines."""
        pass

    @abstractmethod
    def run_pipeline(self, pipeline_name: str, **kwargs) -> None:
        """Run a specific pipeline."""
        pass


class DltAdapter(ExtractLoadAdapterBase):
    def discover_pipelines(self) -> t.Dict[str, t.Callable]:
        """Discover all extract-load pipelines in main.py."""
        pipelines = {}
        main_script = self.package.path / "main.py"
        if main_script.exists():
            pipelines.update(self.package._load_scripts_from_module(main_script))

        return pipelines

    def run_pipeline(self, pipeline_name: str, **kwargs) -> None:
        """Run a specific pipeline."""
        pipelines = self.discover_pipelines()
        if pipeline_name not in pipelines:
            raise ValueError(f"Pipeline {pipeline_name} not found in package {self.package.name}")
        pipelines[pipeline_name](**kwargs)


class SlingAdapter(ExtractLoadAdapterBase):
    def discover_pipelines(self) -> t.Dict[str, t.Callable]:
        """Discover all pipelines for sling."""
        pipelines = {}
        main_script = self.package.path / "main.py"
        if main_script.exists():
            pipelines.update(self.package._load_scripts_from_module(main_script))

        return pipelines

    def run_pipeline(self, pipeline_name: str, **kwargs) -> None:
        """Run a specific pipeline."""
        pipelines = self.discover_pipelines()
        if pipeline_name not in pipelines:
            raise ValueError(f"Pipeline {pipeline_name} not found in package {self.package.name}")
        pipelines[pipeline_name](**kwargs)


class SingerAdapter(ExtractLoadAdapterBase):
    def discover_pipelines(self) -> t.Dict[str, t.Callable]:
        """Singer doesn't have callable pipelines; define commands."""
        return {"default_pipeline": self.run_pipeline}

    def run_pipeline(self, pipeline_name: str = "default_pipeline", **kwargs) -> None:
        """Run a singer pipeline using subprocess."""
        tap = self.package.config.get("singer_tap")
        target = self.package.config.get("singer_target")
        if not tap or not target:
            raise ValueError("Singer adapter requires 'singer_tap' and 'singer_target' in config.")
        subprocess.run(["echo", "1"], check=True)  # nosec
