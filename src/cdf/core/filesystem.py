"""An adapter interface for filesystems."""

import fsspec


def load_filesystem(provider: str, **kwargs) -> fsspec.AbstractFileSystem:
    """Load a filesystem from a provider and kwargs.

    Args:
        provider: The filesystem provider.
        kwargs: The filesystem provider kwargs.

    Returns:
        The filesystem.
    """
    return fsspec.filesystem(provider, **kwargs)
