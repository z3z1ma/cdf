"""Base specification classes for continuous data framework components"""

import ast
import importlib
import inspect
import operator
import os
import runpy
import sys
import typing as t
from contextlib import suppress
from pathlib import Path

import pydantic
from croniter import croniter

import cdf.core.logger as logger

T = t.TypeVar("T")

_NO_DESCRIPTION = "No description provided."
"""A default description for components if not provided or parsed."""


def _gen_anon_name() -> str:
    """Generate an anonymous name for a component."""
    return f"anon_{os.urandom(8).hex()}"


def _getmodulename(name: str) -> str:
    """Wraps inspect.getmodulename to ensure a module name is returned."""
    rv = inspect.getmodulename(name)
    return rv or name


class BaseComponent(pydantic.BaseModel):
    """
    A component specification.

    Components are the building blocks of a data platform. They declaratively describe
    the functions within a workspace which extract, load, transform, and publish data.
    """

    name: t.Annotated[
        str,
        pydantic.Field(
            ...,
            default_factory=_gen_anon_name,
            pattern=r"^[a-zA-Z0-9_-]+$",
            max_length=64,
        ),
    ]
    """The name of the component."""
    version: t.Annotated[int, pydantic.Field(1, ge=1, le=999, frozen=True)] = 1
    """The version of the component."""
    owner: str | None = None
    """The owners of the component."""
    description: str = _NO_DESCRIPTION
    """The description of the component."""
    tags: t.List[str] = []
    """Tags for this component used for component queries and integrations."""
    enabled: bool = True
    """Whether this component is enabled."""
    meta: t.Dict[str, t.Any] = {}
    """Arbitrary user-defined metadata for this component."""

    @property
    def versioned_name(self) -> str:
        """Get the versioned name of the component."""
        return f"{self.name}_v{self.version}"

    @property
    def owners(self) -> t.List[str]:
        """Get the owners."""
        if not self.owner:
            return []
        return self.owner.split(",")

    @pydantic.field_validator("tags", mode="before")
    @classmethod
    def _tags_validator(cls, tags: t.Any) -> t.Sequence[str]:
        """Wrap tags in a list."""
        if isinstance(tags, str):
            tags = tags.split(",")
        return tags

    @pydantic.field_validator("owner", mode="before")
    @classmethod
    def _owner_validator(cls, owner: t.Any) -> str:
        """Ensure owner is a string."""
        if isinstance(owner, (list, tuple)):
            owner = ",".join(owner)
        return owner

    @pydantic.field_validator("description", mode="after")
    @classmethod
    def _description_validator(cls, description: str) -> str:
        """Ensure the description has no leading whitespace."""
        return inspect.cleandoc(description)

    @pydantic.model_validator(mode="before")  # type: ignore
    @classmethod
    def _spec_validator(cls, data: t.Any) -> t.Any:
        """Perform validation on the spec ensuring forward compatibility."""
        if isinstance(data, dict):
            owners = data.pop("owners", None)
            if owners is not None:
                data["owner"] = ",".join(owners)
        return data

    @pydantic.model_validator(mode="after")
    def _setup(self):
        """Import the entrypoint and register the component."""
        if not self.enabled:
            logger.info(f"Skipping disabled component: {self.name}")
            return self
        return self


class WorkspaceComponent(BaseComponent):
    """A component within a workspace."""

    workspace_path: Path = Path(".")
    """The path to the workspace containing the component."""
    component_path: Path = pydantic.Field(Path("."), alias="path")
    """The path to the component within the workspace folder."""

    _folder: str = "."
    """The folder within the workspace. Set per component type during subclassing."""

    @property
    def path(self) -> Path:
        """Get the path to the component."""
        return self.workspace_path / self.component_path

    @pydantic.field_validator("workspace_path", mode="before")
    @classmethod
    def _workspace_path_validator(cls, path: t.Any) -> Path:
        """Ensure the workspace path is a Path."""
        return Path(path).resolve()

    @pydantic.field_validator("component_path", mode="before")
    @classmethod
    def _component_path_validator(cls, component_path: t.Any) -> Path:
        """Ensure the component path is a Path."""
        path = Path(component_path)
        if path.is_absolute():
            raise ValueError("Component path must be a relative path.")
        ns = cls._folder.default  # type: ignore
        if path.parts[0] != ns:
            path = Path(ns) / path
        return path


class Schedulable(pydantic.BaseModel):
    """A mixin for schedulable components."""

    cron_string: str = pydantic.Field("@daily", serialization_alias="cron")
    """A cron expression for scheduling the primary action associated with the component."""

    @property
    def cron(self) -> croniter | None:
        """Get the croniter instance."""
        if self.cron_string is None:
            return None
        return croniter(self.cron_string)  # TODO: add start time here based on last run

    def next_run(self) -> t.Optional[int]:
        """Get the next run time for the component."""
        if self.cron is None:
            return None
        return self.cron.get_next()

    def is_due(self) -> bool:
        """Check if the component is due to run."""
        if self.cron is None:
            return False
        return self.cron.get_next() <= self.cron.get_current()

    @pydantic.field_validator("cron_string", mode="before")
    @classmethod
    def _cron_validator(cls, cron_string: t.Any) -> str:
        """Ensure the cron expression is valid."""
        if isinstance(cron_string, croniter):
            return " ".join(cron_string.expressions)
        elif isinstance(cron_string, str):
            try:
                croniter(cron_string)
            except Exception as e:
                raise ValueError(f"Invalid cron expression: {cron_string}") from e
            else:
                return cron_string
        raise TypeError(
            f"Invalid cron type: {type(cron_string)} is not str or croniter."
        )


class InstallableRequirements(pydantic.BaseModel):
    """A mixin for components that support installation of requirements."""

    requirements: t.List[t.Annotated[str, pydantic.Field(..., frozen=True)]] = []
    """The requirements for the component."""

    @pydantic.field_validator("requirements", mode="before")
    @classmethod
    def _requirements_validator(cls, requirements: t.Any) -> t.Sequence[str]:
        """Wrap requirements in a list."""
        if isinstance(requirements, str):
            requirements = requirements.split(",")
        return requirements

    def install_requirements(self) -> None:
        """Install the component."""
        if not self.requirements:
            return
        name = getattr(self, "name", self.__class__.__name__)
        logger.info(f"Installing requirements for {name}: {self.requirements}")
        try:
            import pip
        except ImportError:
            raise ImportError(
                "Pip was not found. Please install pip or recreate the virtual environment."
            )
        pip.main(["install", *self.requirements])


class PythonScript(WorkspaceComponent, InstallableRequirements):
    """A python script component."""

    @pydantic.model_validator(mode="after")
    def _setup(self):
        """Import the entrypoint and register the component."""
        if self.name.startswith("anon_"):
            self.name = self.name.replace("anon_", self.path.stem)
        if self.description == _NO_DESCRIPTION:
            tree = ast.parse(self.path.read_text())
            self.description = ast.get_docstring(tree) or _NO_DESCRIPTION
        return self

    def package(self, outputdir: str) -> None:
        """Package the component."""
        from pex.bin import pex

        name = getattr(self, "name", self.__class__.__name__)
        logger.info(f"Packaging {name}...")

        output = os.path.join(outputdir, f"{name}.pex")
        try:
            # --inject-env in pex can add the __cdf_name__ variable?
            # or really any other variable that should be injected
            pex.main(["-o", output, ".", *self.requirements])
        except SystemExit as e:
            # A failed pex build will exit with a non-zero code
            # Successfully built pexes will exit with either 0 or None
            if e.code is not None and e.code != 0:
                # If the pex fails to build, delete the compromised pex
                with suppress(FileNotFoundError):
                    os.remove(output)
                raise

    @property
    def main(self) -> t.Callable[[], t.Any]:
        """Get the entrypoint function."""

        def _run() -> t.Any:
            """Run the script"""
            origpath = sys.path[:]
            sys.path = [
                str(self.workspace_path),
                *sys.path,
                str(self.workspace_path.parent),
            ]
            parts = map(
                _getmodulename, self.path.relative_to(self.workspace_path).parts
            )
            run_name = ".".join(parts)
            try:
                return runpy.run_path(
                    str(self.path),
                    run_name=run_name,
                    init_globals={
                        "__file__": str(self.path),
                        "__cdf_name__": run_name,
                    },
                )
            finally:
                sys.path = origpath

        return _run

    def __call__(self) -> t.Any:
        """Run the script."""
        return self.main()


class PythonEntrypoint(BaseComponent, InstallableRequirements):
    """A python entrypoint component."""

    entrypoint: t.Annotated[
        str,
        pydantic.Field(..., frozen=True, pattern=r"^[a-zA-Z0-9_\.]+:[a-zA-Z0-9_\.]+$"),
    ]
    """The entrypoint of the component."""

    @pydantic.model_validator(mode="after")
    def _setup(self):
        """Import the entrypoint and register the component."""
        if self.name.startswith("anon_"):
            mod, func = self.entrypoint.split(":", 1)
            self.name = mod.replace(".", "_") + "_" + func.replace(".", "_")
        if self.description == _NO_DESCRIPTION:
            self.description = self.main.__doc__ or _NO_DESCRIPTION
        return self

    @property
    def main(self) -> t.Callable[..., t.Any]:
        """Get the entrypoint function."""
        module, func = self.entrypoint.split(":")
        mod = importlib.import_module(module)
        return operator.attrgetter(func)(mod)

    def __call__(self, *args: t.Any, **kwargs: t.Any) -> t.Any:
        """Run the entrypoint."""
        return self.main(*args, **kwargs)


class CanExecute(t.Protocol):
    """A protocol specifying the minimum interface executable components satisfy."""

    @property
    def main(self) -> t.Callable[..., t.Any]: ...

    def __call__(self, *args: t.Any, **kwargs: t.Any) -> t.Any: ...


__all__ = [
    "BaseComponent",
    "Schedulable",
    "PythonScript",
    "PythonEntrypoint",
    "WorkspaceComponent",
    "CanExecute",
]
