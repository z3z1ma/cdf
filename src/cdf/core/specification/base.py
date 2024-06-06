"""Base specification classes for continuous data framework components"""

from __future__ import annotations

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
from threading import Lock

import pydantic
from croniter import croniter

import cdf.core.constants as c
import cdf.core.logger as logger

if t.TYPE_CHECKING:
    from cdf.core.project import Workspace

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


class BaseComponent(
    pydantic.BaseModel, use_attribute_docstrings=True, from_attributes=True
):
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
    """The name of the component. Must be unique within the workspace."""
    version: t.Annotated[int, pydantic.Field(1, ge=1, le=999, frozen=True)] = 1
    """The version of the component.

    Used internally to version datasets and serves as an external signal to dependees that something
    has changed in a breaking way. All components are versioned.
    """
    owner: t.Optional[str] = None
    """The owners of the component."""
    description: str = _NO_DESCRIPTION
    """The description of the component.

    This should help users understand the purpose of the component. For scripts and entrypoints, we
    will attempt to extract the relevant docstring.
    """
    tags: t.List[str] = []
    """Tags for this component used for component queries and integrations."""
    enabled: bool = True
    """Whether this component is enabled. Respected in cdf operations."""
    meta: t.Dict[str, t.Any] = {}
    """Arbitrary user-defined metadata for this component.

    Used for user-specific integrations and automation.
    """

    _workspace: t.Optional["Workspace"] = None
    """The workspace containing the component. Set by the workspace model validator."""

    @property
    def workspace(self) -> "Workspace":
        """Get the workspace containing the component."""
        if self._workspace is None:
            raise ValueError("Component not associated with a workspace.")
        return self._workspace

    @property
    def has_workspace_association(self) -> bool:
        """Check if the component has a workspace association."""
        return self._workspace is not None

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
    def _setup_base(self):
        """Import the entrypoint and register the component."""
        if not self.enabled:
            logger.info(f"Skipping disabled component: {self.name}")
            return self
        return self

    def __getitem__(self, key: str) -> t.Any:
        """Get a field from the component."""
        if key not in self.model_fields:
            raise KeyError(f"No attribute {key} found in component {self.name}")
        try:
            return getattr(self, key)
        except AttributeError as e:
            raise KeyError(f"Attribute {key} not found in component {self.name}") from e


class WorkspaceComponent(BaseComponent):
    """A component within a workspace."""

    workspace_path: t.Annotated[Path, pydantic.Field(frozen=True)] = Path(".")
    """The path to the workspace within the project folder."""
    component_path: t.Annotated[Path, pydantic.Field(alias="path", frozen=True)]
    """The path to the component within the workspace folder."""

    _folder: str = "."
    """The folder within the workspace where components are stored."""
    _extension: str = "py"
    """The extension for components of this type."""

    @property
    def path(self) -> Path:
        """Get the path to the component."""
        return self.workspace_path / self.component_path

    @pydantic.model_validator(mode="before")
    @classmethod
    def _infer_leaf_path_validator(cls, values: t.Any) -> t.Any:
        """Infer the leaf path from the name if component_path is not provided.

        Given a name, we apply certain heuristics to infer the path of the component if a
        path is not explicitly provided. The heuristics are as follows:
        - If the name ends with the component extension (.py), we use the name as the path.
        - If the name does NOT end with the component extension, we append the component type
          if not present. So a pipeline name like `darksky` would become `darksky_pipeline`.
        - We then append the component extension and set the path. So `darksky_pipeline.py`

        The _component_path_validator validator is uniformly responsible for prefixing the
        folder name to the path.
        """
        name = values.get("name")
        if not name:
            return values
        if name.endswith((".py", ".ipynb")):
            values.setdefault("path", name)
        else:
            ext = getattr(cls._extension, "default")
            typ = getattr(cls._folder, "default")[:-1]
            if name.endswith(f"_{typ}"):
                leaf_path = f"{name}.{ext}"
            else:
                leaf_path = f"{name}_{typ}.{ext}"
            values.setdefault("path", leaf_path)
        return values

    @pydantic.field_validator("name", mode="before")
    @classmethod
    def _physical_name_validator(cls, name: t.Any) -> t.Any:
        """Canonicalizes names which are pathlike.

        So a name like `some/path/to/file.py` would become `some_path_to_file`.
        """
        if isinstance(name, str):
            return name.rsplit(".", 1)[0].replace(os.sep, "_")
        return name

    @pydantic.field_validator("component_path", mode="before")
    @classmethod
    def _component_path_validator(cls, component_path: t.Any) -> Path:
        """Ensure the component path is a Path and that is a child of the expected folder."""
        path = Path(component_path)
        if path.is_absolute():
            raise ValueError("Component path must be a relative path.")
        ns = getattr(cls._folder, "default")
        if path.parts[0] != ns:
            path = Path(ns) / path
        return path


class Schedulable(pydantic.BaseModel):
    """A mixin for schedulable components."""

    cron_string: t.Annotated[
        str, pydantic.Field(serialization_alias="cron", frozen=True)
    ] = "@daily"
    """A cron expression for scheduling the primary action associated with the component.

    This is intended to be leveraged by libraries like Airflow.
    """

    @property
    def cron(self) -> t.Optional[croniter]:
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

    requirements: t.Annotated[t.List[str], pydantic.Field(frozen=True)] = []
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

    auto_install: bool = False
    """Whether to automatically install the requirements for the script. 

    Useful for leaner Docker images which defer certain component dep installs to runtime.
    """

    _lock: Lock = pydantic.PrivateAttr(default_factory=Lock)
    """A lock for ensuring thread safety."""

    @pydantic.model_validator(mode="after")
    def _setup_script(self):
        """Import the entrypoint and register the component."""
        if self.name.startswith("anon_"):
            self.name = self.name.replace("anon_", self.path.stem)
        if self.description == _NO_DESCRIPTION:
            tree = ast.parse(self.path.read_text())
            with suppress(TypeError):
                self.description = ast.get_docstring(tree) or _NO_DESCRIPTION
        return self

    def package(self, outputdir: str) -> None:
        """Package the component."""
        from pex.bin import pex

        name = getattr(self, "name", self.__class__.__name__)
        logger.info(f"Packaging {name}...")

        output = os.path.join(outputdir, f"{name}.pex")
        try:
            # --inject-env in pex can add the c.CDF_MAIN variable?
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
    def main(self) -> t.Callable[[], t.Dict[str, t.Any]]:
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
                with self._lock:
                    if self.auto_install:
                        self.install_requirements()
                    return runpy.run_path(
                        str(self.path),
                        run_name=run_name,
                        init_globals={
                            "__file__": str(self.path),
                            c.CDF_MAIN: run_name,
                        },
                    )
            except SystemExit as e:
                if e.code != 0:
                    raise
                return {}
            except Exception as e:
                logger.exception(f"Error running script {self.name}: {e}")
                raise
            finally:
                sys.path = origpath

        return _run

    def __call__(self) -> t.Dict[str, t.Any]:
        """Run the script."""
        return self.main()


class PythonEntrypoint(BaseComponent, InstallableRequirements):
    """A python entrypoint component."""

    entrypoint: t.Annotated[
        str,
        pydantic.Field(
            ...,
            frozen=True,
            pattern=r"^[a-zA-Z0-9_\.]+:[a-zA-Z0-9_\.]+$",
            description="The entrypoint function in the format module:func.",
        ),
    ]
    """The entrypoint of the component."""

    @pydantic.model_validator(mode="after")
    def _setup_entrypoint(self):
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
