"""The registry contains all user defined sources."""
import typing as t

from cdf.core.exception import RegistryTypeError, SourceNotFoundError

if t.TYPE_CHECKING:
    from cdf.core.source import ContinuousDataFlowSource
else:
    ContinuousDataFlowSource = t.Any


_sources: t.Dict[str, ContinuousDataFlowSource] = {}


def register_source(source: ContinuousDataFlowSource) -> None:
    """Register a source class with the registry."""
    from cdf.core.source import ContinuousDataFlowSource

    if not isinstance(source, ContinuousDataFlowSource):
        raise RegistryTypeError(
            f"Expected a ContinuousDataFlowSource, got {type(source)}"
        )
    _sources[source.name] = source


def get_source(name: str) -> ContinuousDataFlowSource:
    """Get a source class from the registry."""
    try:
        return _sources[name]
    except KeyError:
        raise SourceNotFoundError(f"Source {name} not found in registry.")


def get_source_names() -> t.List[str]:
    """Get a list of registered source names."""
    return list(_sources.keys())


def get_sources() -> t.List[ContinuousDataFlowSource]:
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


def __getattr__(name: str) -> ContinuousDataFlowSource:
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
]

__dir__ = lambda: list(_sources.keys()) + __all__
