"""Base specification classes for continuous data framework components"""
import functools
import hashlib
import importlib
import inspect
import os
import sys
import types
import typing as t
from collections import defaultdict

import pydantic
from croniter import croniter

import cdf.core.constants as c
import cdf.core.context as context
import cdf.core.logger as logger

_NO_DESCRIPTION = "No description provided."
"""A default description for components if not provided or parsed."""


def _gen_anon_name() -> str:
    """Generate an anonymous name for a component."""
    return f"anon_{os.urandom(8).hex()}"


def _get_name_from_func(func: t.Any) -> str | None:
    """Get the name from the entrypoint function on a best effort basis."""
    if isinstance(func, functools.partial):
        return _get_name_from_func(func.func)
    name = None
    obj = getattr(func, "__wrapped__", func)
    if hasattr(obj, "name"):
        name = obj.name
    elif hasattr(obj, "__name__"):
        name = obj.__name__
    elif hasattr(obj, "__class__"):
        name = obj.__class__.__name__
    elif hasattr(obj, "__qualname__"):
        name = obj.__qualname__
    return name


def _get_name_from_entrypoint(entrypoint: str) -> str:
    """Get the name from an entrypoint string."""
    _, func = entrypoint.split(":")
    return func.replace(".", "_")


def _get_description_from_func(func: t.Any) -> str:
    """Get the description from the entrypoint function on a best effort basis."""
    if isinstance(func, functools.partial):
        return _get_description_from_func(func.func)
    obj = getattr(func, "__wrapped__", func)
    description = inspect.getdoc(obj)
    if not description:
        mod = inspect.getmodule(obj)
        description = mod.__doc__ if mod else None
    return description or _NO_DESCRIPTION


# TODO: we need to track the last execution time of the component to use as the croniter `start_time`
class Schedulable(pydantic.BaseModel):
    """A mixin for schedulable components."""

    cron_: str | None = pydantic.Field(None, alias="cron")
    """A cron expression for scheduling the primary action associated with the component."""

    @property
    def cron(self) -> croniter | None:
        """Get the croniter instance."""
        if self.cron_ is None:
            return None
        return croniter(self.cron_)

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

    @pydantic.field_validator("cron_", mode="before")
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


class Executable(pydantic.BaseModel):
    """A mixin for entrypoint based executable components."""

    entrypoint: t.Annotated[
        str,
        pydantic.Field(..., pattern=r"^.*:.*$", frozen=True),
    ]
    """The entrypoint of the component."""

    _main: t.Callable | None = None
    """Container for the entrypoint function."""

    @property
    def main(self) -> t.Callable[..., t.Any]:
        """Get the entrypoint function."""
        if self._main is None:
            self._main = self.load_entrypoint()
        return self._main

    def load_entrypoint(self) -> t.Any:
        """Load the entrypoint."""
        mod, func = self.entrypoint.split(":")
        try:
            module = importlib.import_module(mod, getattr(self, "_key", None))
        except ModuleNotFoundError:
            raise ValueError(f"Module {mod} not found.")
        except Exception as e:
            raise ValueError(f"Error importing module {mod}: {e}")
        try:
            func = getattr(module, func)
        except AttributeError:
            raise ValueError(f"Function {func} not found in module {mod}.")
        except Exception as e:
            raise ValueError(f"Error importing function {func} from module {mod}: {e}")
        return func


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

    _key: c.TComponents | None = None
    """Determines the namespace of the component in the registry"""
    _autoregister: bool = True
    """Whether to autoregister the component."""

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
        workspace = context.get_active_workspace()

        if isinstance(self, Packageable) and context.is_autoinstall_enabled():
            # TODO: this is a cheap fast implementation until we overhaul packaging as a concept
            if workspace:
                hashkey = hashlib.md5(self.__class__.__name__.encode())
                hashkey.update(self.versioned_name.encode())
                hashkey.update(str(sorted(self.requirements)).encode())
                touchfile = workspace.root / ".cdf" / hashkey.hexdigest()
                touchfile.parent.mkdir(exist_ok=True, parents=True)
                if not touchfile.exists():
                    self.install()
                touchfile.touch()
            else:
                self.install()

        if isinstance(self, Executable):
            if self.name.startswith("anon_"):
                n = _get_name_from_func(self.main) or _get_name_from_entrypoint(
                    self.entrypoint
                )
                self.name = n or self.name
            if self.description == _NO_DESCRIPTION:
                self.description = _get_description_from_func(self.main)

        if workspace and self._autoregister:
            ComponentSpecification.register(self, workspace.name)

        return self

    @property
    def entrypoint_info(self) -> str:
        """Convert an entrypoint function to a useful string representation."""
        if not isinstance(self, Executable):
            return "N/A"
        fn = self._main
        if fn is None:
            fn = self.load_entrypoint()
        if isinstance(fn, functools.partial):
            fn = fn.func
        if hasattr(fn, "name"):
            return f"object: {type(fn)}({fn.name}), id: {id(fn)}"
        elif isinstance(fn, types.GeneratorType):
            return f"object: gen({fn.__name__}), id: {id(fn)}"
        elif callable(fn):
            mod = inspect.getmodule(fn)
            if mod:
                mod = mod.__name__
            _, lineno = inspect.getsourcelines(fn)
            return f"mod: {mod}, fn: {fn.__name__}, ln: {lineno}"
        return str(fn)

    @staticmethod
    def register(
        component: "ComponentSpecification", namespace: str = c.DEFAULT_WORKSPACE
    ) -> None:
        """Export this component to the global registry under a specific namespace."""
        CDF_REGISTRY[namespace][component._key][component.name] = component

    @staticmethod
    def register_subcomponent(
        component: "ComponentSpecification",
        parent_name: str,
        namespace: str = c.DEFAULT_WORKSPACE,
    ) -> None:
        """Export this subcomponent to the global registry under a specific namespace."""
        name = f"{parent_name}.{component.name}"
        inputs = getattr(component, "input_", None)
        if inputs:
            name = f"{name}.{hash(tuple(inputs.items())) + sys.maxsize + 1}"
        CDF_REGISTRY[namespace][component._key][name] = component


T = t.TypeVar("T", bound=ComponentSpecification)


class ComponentRegistry(t.Dict[str, T]):
    """A registry of unique components."""

    def __setitem__(self, key: str, spec: T) -> None:
        if not isinstance(spec, ComponentSpecification):
            raise TypeError(
                f"ComponentRegistry only accepts instances of ComponentSpecification, not {type(spec)}"
            )
        if key in self:
            raise KeyError(f"Component {key} already registered in registry.")
        super().__setitem__(key, spec)

    def __getitem__(self, key: str) -> T:
        try:
            return super().__getitem__(key)
        except KeyError:
            raise KeyError(f"Component {key} not found in registry.")


CDF_REGISTRY: t.Dict[
    str, t.Dict[c.TComponents | None, ComponentRegistry]
] = defaultdict(lambda: {key: ComponentRegistry() for key in c.COMPONENTS})
"""
The global component registry. Components are registered here when they are exported.

Components are organized by 2 orders of namespace. The first is a user-defined, public namespace
which is used to break up components into logical groups. In CDF, this corresponds to the workspace
name. The second is an internal namespace which is used to break up components into functional
groups. This is based on the component type and the key is based on the private _key attribute of
the component specification.
"""


class SupportsComponentMetadata(t.Protocol):
    """A minimal protocol for components which support metadata."""

    name: str
    description: t.Any
    owner: t.Any
    tags: t.List[str]
    cron: t.Any


__all__ = [
    "ComponentSpecification",
    "ComponentRegistry",
    "CDF_REGISTRY",
    "Schedulable",
    "Packageable",
    "Executable",
    "SupportsComponentMetadata",
]
