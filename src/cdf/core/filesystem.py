"""An adapter interface for filesystems."""

import typing as t

import fsspec
import fsspec.implementations.dirfs as dirfs
from dlt.common.configuration import with_config


@with_config(sections=("filesystem",))
def load_filesystem_provider(
    provider: t.Optional[str] = None,
    path: t.Optional[str] = None,
    options: t.Optional[t.Dict[str, t.Any]] = None,
) -> fsspec.AbstractFileSystem:
    """Load a filesystem from a provider and kwargs.

    Args:
        provider: The filesystem provider.
        options: The filesystem provider kwargs.

    Returns:
        The filesystem.
    """
    return dirfs.DirFileSystem(
        path,
        target_protocol=provider or "file",
        target_options=options or {},
        auto_mkdir=True,
    )
