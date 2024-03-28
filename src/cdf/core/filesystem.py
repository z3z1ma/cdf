"""An adapter interface for filesystems."""

import typing as t

import fsspec
from dlt.common.configuration import with_config


@with_config(sections=("filesystem",))
def load_filesystem_provider(
    provider: t.Optional[str] = None, options: t.Optional[t.Dict[str, t.Any]] = None
) -> fsspec.AbstractFileSystem:
    """Load a filesystem from a provider and kwargs.

    Args:
        provider: The filesystem provider.
        kwargs: The filesystem provider kwargs.

    Returns:
        The filesystem.
    """
    options = options or {}
    return fsspec.filesystem(provider, **options)
