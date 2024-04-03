"""An adapter interface for filesystems."""

import typing as t

import fsspec
import fsspec.implementations.dirfs as dirfs
from dlt.common.configuration import with_config

from cdf.types import PathLike


@with_config(sections=("filesystem",))
def load_filesystem_provider(
    provider: t.Optional[str] = None,
    root: t.Optional[PathLike] = None,
    options: t.Optional[t.Dict[str, t.Any]] = None,
) -> fsspec.AbstractFileSystem:
    """Load a filesystem from a provider and kwargs.

    Args:
        provider: The filesystem provider.
        root: The root path for the filesystem.
        options: The filesystem provider kwargs.

    Returns:
        The filesystem.
    """
    return dirfs.DirFileSystem(
        root or "_storage",
        target_protocol=provider or "file",
        target_options=options or {},
        auto_mkdir=True,
    )
