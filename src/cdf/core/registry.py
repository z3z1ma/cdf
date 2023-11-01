"""The registry contains all active user defined sources.

They are registered at runtime by the user either manually or by calling a lazy
source loaded by a SourceLoader.
"""
import typing as t

from cdf.core.exception import RegistryTypeError, SourceNotFoundError

if t.TYPE_CHECKING:
    from cdf.core.source import CDFSource


_sources: t.Dict[str, "CDFSource"] = {}


def register_source(source: "CDFSource") -> None:
    """Register a source class with the registry."""
    from cdf.core.source import CDFSource

    if not isinstance(source, CDFSource):
        raise RegistryTypeError(f"Expected a CDFSource, got {type(source)}")
    _sources[source.name] = source


def get_source(name: str) -> "CDFSource":
    """Get a source class from the registry."""
    try:
        return _sources[name]
    except KeyError:
        raise SourceNotFoundError(f"Source {name} not found in registry.")


def get_source_names() -> t.List[str]:
    """Get a list of registered source names."""
    return list(_sources.keys())


def get_sources() -> t.List["CDFSource"]:
    """Get a list of registered source classes."""
    return list(_sources.values())


def remove_source(name: str) -> None:
    """Remove a source class from the registry."""
    try:
        del _sources[name]
    except KeyError:
        raise SourceNotFoundError(f"Source {name} not found in registry.")


def has_source(name: str) -> bool:
    """Check if a source is in the registry."""
    return name in _sources


def __getattr__(name: str) -> "CDFSource":
    """Get a source class from the registry."""
    if name in _sources:
        return _sources[name]
    raise SourceNotFoundError(f"Source {name} not found in registry.")


__all__ = [
    "register_source",
    "remove_source",
    "get_source",
    "get_source_names",
    "get_sources",
    "has_source",
]


def __dir__() -> t.List[str]:
    return list(_sources.keys()) + __all__
