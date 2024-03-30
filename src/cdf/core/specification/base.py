"""Base specification classes for continuous data framework components"""

import ast
import importlib
import inspect
import operator
import os
import runpy
import typing as t
from pathlib import Path

import pydantic
from croniter import croniter

import cdf.core.logger as logger

_NO_DESCRIPTION = "No description provided."
"""A default description for components if not provided or parsed."""


def _gen_anon_name() -> str:
    """Generate an anonymous name for a component."""
    return f"anon_{os.urandom(8).hex()}"


class ComponentSpecification(pydantic.BaseModel):
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

        if isinstance(self, PythonScript):
            if self.name.startswith("anon_"):
                self.name = self.name.replace("anon_", self.script_path.stem)
            if self.description == _NO_DESCRIPTION:
                tree = ast.parse(self.script_path.read_text())
                self.description = ast.get_docstring(tree) or _NO_DESCRIPTION

        return self


# TODO: we need to track the last execution time of the component to use as the croniter `start_time`
class Schedulable(pydantic.BaseModel):
    """A mixin for schedulable components."""

    cron_string: str | None = pydantic.Field(None, alias="cron")
    """A cron expression for scheduling the primary action associated with the component."""

    @property
    def cron(self) -> croniter | None:
        """Get the croniter instance."""
        if self.cron_string is None:
            return None
        return croniter(self.cron_string)  # add start time here based on last run

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
    def _cron_validator(cls, cron: t.Any) -> str:
        """Ensure the cron expression is valid."""
        if isinstance(cron, croniter):
            return " ".join(cron.expressions)
        elif isinstance(cron, str):
            try:
                croniter(cron)
                return cron
            except Exception as e:
                raise ValueError(f"Invalid cron expression: {cron}") from e
        raise TypeError(f"Invalid cron type: {type(cron)} is not str or croniter.")


class Packageable(pydantic.BaseModel):
    """A mixin for packageable components."""

    requirements: t.List[t.Annotated[str, pydantic.Field(..., frozen=True)]] = []

    def install(self) -> None:
        """Install the component."""
        if not self.requirements:
            return
        name = getattr(self, "name", self.__class__.__name__)
        logger.info(f"Installing requirements for {name}: {self.requirements}")
        try:
            import pip
        except ImportError:
            raise ImportError(
                "pip is required to install requirements. Please install pip."
            )
        pip.main(["install", *self.requirements])

    def package(self, outputdir: str) -> None:
        """Package the component."""
        from pex.bin import pex

        name = getattr(self, "name", self.__class__.__name__)
        logger.info(f"Packaging {name}...")

        output = os.path.join(outputdir, f"{name}.pex")
        try:
            pex.main(["-o", output, ".", *self.requirements])
        except SystemExit as e:
            # A failed pex build will exit with a non-zero code
            # Successfully built pexes will exit with either 0 or None
            if e.code is not None and e.code != 0:
                # If the pex fails to build, delete the compromised pex
                try:
                    os.remove(output)
                except FileNotFoundError:
                    pass
                raise

    @pydantic.field_validator("requirements", mode="before")
    @classmethod
    def _requirements_validator(cls, requirements: t.Any) -> t.Sequence[str]:
        """Wrap requirements in a list."""
        if isinstance(requirements, str):
            requirements = requirements.split(",")
        return requirements


class PythonScript(pydantic.BaseModel):
    """A mixin for script based executable components."""

    script_path: Path
    """The path to the script"""

    @property
    def main(self) -> t.Callable[..., t.Any]:
        """Get the entrypoint function."""

        def _run() -> t.Any:
            """Run the script"""
            return runpy.run_path(str(self.script_path), run_name="__main__")

        return _run


class PythonEntrypoint(pydantic.BaseModel):
    """A mixin for entrypoint based executable components."""

    entrypoint: t.Annotated[
        str, pydantic.Field(..., frozen=True, pattern=r"^[a-zA-Z0-9_]+:[a-zA-Z0-9_]+$")
    ]
    """The entrypoint of the component."""

    @property
    def main(self) -> t.Callable[..., t.Any]:
        """Get the entrypoint function."""

        def _run(*args: t.Any, **kwargs: t.Any) -> t.Any:
            """Run the script"""
            module, func = self.entrypoint.split(":")
            mod = importlib.import_module(module)
            return operator.attrgetter(func)(mod)(*args, **kwargs)

        return _run


__all__ = [
    "ComponentSpecification",
    "Schedulable",
    "Packageable",
    "PythonScript",
]
