import importlib
import inspect
import typing as t
from contextlib import suppress
from dataclasses import field
from enum import Enum

import pydantic

import cdf.core.context as ctx
import cdf.core.injector as injector

if t.TYPE_CHECKING:
    from cdf.core.workspace import Workspace

T = t.TypeVar("T")

__all__ = [
    "Component",
    "Entrypoint",
    "ServiceLevelAgreement",
]


class ServiceLevelAgreement(Enum):
    """An SLA to assign to a component. Users can define the meaning of each level."""

    DEPRECATING = -1
    NONE = 0
    LOW = 1
    MEDIUM = 2
    HIGH = 3
    CRITICAL = 4


class _Node(pydantic.BaseModel, frozen=True):
    """A node in a graph of components."""

    owner: t.Optional[str] = None
    """The owner of the node. Useful for tracking who to contact for issues or config."""
    description: str = "No description provided"
    """A description of the node."""
    sla: ServiceLevelAgreement = ServiceLevelAgreement.MEDIUM
    """The SLA for the node."""
    enabled: bool = True
    """Whether the node is enabled or disabled. Disabled components are not loaded."""
    version: str = "0.1.0"
    """A semantic version for the node. Can signal breaking changes to dependents."""
    tags: t.List[str] = field(default_factory=list)
    """Tags to categorize the node."""
    metadata: t.Dict[str, t.Any] = field(default_factory=dict)
    """Additional metadata for the node. Useful for custom integrations."""

    @pydantic.field_validator("sla", mode="before")
    @classmethod
    def _validate_sla(cls, value: t.Any) -> t.Any:
        if isinstance(value, str):
            value = ServiceLevelAgreement[value.upper()]
        return value

    @pydantic.field_validator("tags", mode="before")
    @classmethod
    def _validate_tags(cls, value: t.Any) -> t.Any:
        if isinstance(value, str):
            value = value.split(",")
        return value


def _parse_metadata_from_callable(func: t.Callable) -> t.Dict[str, t.Any]:
    """Parse _Node metadata from a function or class allowing looser coupling of configuration.

    The function or class docstring is used as the description if available. The rest
    of the metadata is inferred from the function or class attributes. The attributes
    may be in global form (<attr>) or dunder form (__<attr>__).

    We look for the following attributes in the function or class:
    - name: The name of the component

    The following attributes in the function or class with fallback to the module:
    - version: The version of the component
    - enabled: Whether the component is enabled
    - sla: The SLA of the component
    - owner: The owner of the component

    And the following are merged from both the function and module:
    - tags: Tags for the component
    - metadata: Additional metadata for the component
    """
    if not callable(func):
        return {}

    mod = inspect.getmodule(func)

    def _lookup_attributes(
        *attrs: str, callback: t.Optional[t.Callable[[t.Any], t.Any]] = None
    ) -> t.Optional[t.Any]:
        # Look for the attribute in the function and module
        for attr in attrs:
            with suppress(AttributeError):
                v = getattr(func, attr)
                if callback:
                    callback(v)
                else:
                    return v
            if mod is not None:
                with suppress(AttributeError):
                    v = getattr(mod, attr)
                    if callback:
                        callback(v)
                    else:
                        return v

    parsed: t.Dict[str, t.Any] = {
        "description": inspect.getdoc(func) or "No description provided"
    }
    for k in ("name", "version", "enabled", "sla", "owner"):
        if (v := _lookup_attributes(k.upper(), f"__{k}__")) is not None:
            parsed[k] = v

    _lookup_attributes(
        "TAGS", "__tags__", callback=parsed.setdefault("tags", []).extend
    )
    _lookup_attributes(
        "METADATA", "__metadata__", callback=parsed.setdefault("metadata", {}).update
    )

    return parsed


def _bind_active_workspace(func: t.Any) -> t.Any:
    """Bind the active workspace to a function or class.

    Args:
        func: The function or class to bind the workspace to.

    Returns:
        The bound function or class.
    """
    if callable(func):
        return ctx.resolve(eagerly_bind_workspace=True)(func)
    return func


def _get_bind_func(info: pydantic.ValidationInfo) -> t.Callable:
    """Get the bind function from the pydantic context or use the active workspace.

    Args:
        info: The pydantic validation info.

    Returns:
        The bind function to use for the component.
    """
    context = info.context
    if context:
        bind = t.cast("Workspace", context["parent"]).bind
    else:
        bind = _bind_active_workspace
    return bind


def _unwrap_entrypoint(value: t.Any) -> t.Any:
    """Import an entrypoint if it is a string.

    Args:
        value: The value to import.

    Returns:
        The imported value if it is a string, otherwise the original value.
    """
    if isinstance(value, str):
        mod, func = value.split(":", 1)
        mod = importlib.import_module(mod)
        value = getattr(mod, func)
    return value


class Component(_Node, t.Generic[T], frozen=True):
    """A component with a binding to a dependency."""

    main: injector.Dependency[T]
    """The dependency for the component. This is what is injected into the workspace."""

    name: t.Annotated[str, pydantic.Field(..., pattern=r"^[a-zA-Z_][a-zA-Z0-9_]*$")]
    """The key to register the component in the container. 

    Must be a valid Python identifier. Users can use these names as function parameters
    for implicit dependency injection. Names must be unique within the workspace.
    """

    def __call__(self) -> T:
        """Unwrap the main dependency invoking the underlying callable."""
        return self.main.unwrap()

    @pydantic.model_validator(mode="before")
    @classmethod
    def _parse_main(cls, data: t.Any) -> t.Any:
        """Parse function metadata into node defaults."""
        if inspect.isfunction(data) or isinstance(data, injector.Dependency):
            data = {"main": data}
        if isinstance(data, dict):
            dep = data["main"]
            if isinstance(dep, dict):
                func = dep["factory"]
                if dep.get("alias", None):
                    data.setdefault("name", dep["alias"])
            elif isinstance(dep, injector.Dependency):
                func = dep.factory
                if dep.alias:
                    data.setdefault("name", dep.alias)
            else:
                func = dep
            return {**_parse_metadata_from_callable(func), **data}
        return data

    @pydantic.field_validator("main", mode="before")
    @classmethod
    def _ensure_dependency(cls, value: t.Any, info: pydantic.ValidationInfo) -> t.Any:
        """Ensure the main function is a dependency."""
        value = _unwrap_entrypoint(value)
        if isinstance(value, (dict, injector.Dependency)):
            parsed_dep = injector.Dependency.model_validate(value, context=info.context)
        else:
            parsed_dep = injector.Dependency.wrap(value)
        # NOTE: We do this extra round-trip to bypass the unecessary Generic type check in pydantic
        return parsed_dep.model_dump()

    @pydantic.model_validator(mode="after")
    def _bind_main(self, info: pydantic.ValidationInfo) -> t.Any:
        """Bind the active workspace to the main function."""
        self.main.map(_get_bind_func(info), idempotent=True)
        return self

    def __str__(self):
        return f"<Component {self.name} ({self.sla.name})>"


class Entrypoint(_Node, t.Generic[T], frozen=True):
    """An entrypoint representing an invokeable set of functions."""

    main: t.Callable[..., T]
    """The main function associated with the entrypoint."""

    name: str
    """The name of the entrypoint.

    This is used to register the entrypoint in the workspace and CLI. Names must be
    unique within the workspace. The name can contain spaces and special characters.
    """

    @pydantic.model_validator(mode="before")
    @classmethod
    def _parse_main(cls, data: t.Any) -> t.Any:
        """Parse function metadata into node defaults."""
        if inspect.isfunction(data):
            data = {"main": data}
        if isinstance(data, dict):
            func = _unwrap_entrypoint(data["main"])
            return {**_parse_metadata_from_callable(func), **data}
        return data

    @pydantic.field_validator("main", mode="before")
    @classmethod
    def _bind_main(cls, value: t.Any, info: pydantic.ValidationInfo) -> t.Any:
        """Bind the active workspace to the main function."""
        return _get_bind_func(info)(_unwrap_entrypoint(value))

    def __str__(self):
        return f"<Entrypoint {self.name} ({self.sla.name})>"

    def __call__(self, *args: t.Any, **kwargs: t.Any) -> t.Any:
        """Invoke the entrypoint."""
        return self.main(*args, **kwargs)
