"""An adapter interface for filesystems."""

import fsspec
from dlt.common.configuration import with_config


@with_config(sections=("filesystem",))
def load_filesystem(provider: str, **options) -> fsspec.AbstractFileSystem:
    """Load a filesystem from a provider and kwargs.

    Args:
        provider: The filesystem provider.
        kwargs: The filesystem provider kwargs.

    Returns:
        The filesystem.
    """
    return fsspec.filesystem(provider, **options)
