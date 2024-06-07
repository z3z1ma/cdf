"""An adapter interface for filesystems."""

import posixpath
import typing as t
from functools import cached_property
from pathlib import Path

import fsspec
from dlt.common.configuration import with_config
from fsspec.core import strip_protocol
from fsspec.utils import get_protocol

from cdf.types import M, PathLike

if t.TYPE_CHECKING:
    from cdf.core.project import FilesystemSettings


class FilesystemAdapter:
    """Wraps an fsspec filesystem.

    The filesystem is lazily loaded. Certain methods are intercepted to include cdf-specific logic. Helper
    methods are provided for specific operations.
    """

    @with_config(sections=("filesystem",))
    def __init__(
        self,
        uri: t.Optional[PathLike] = None,
        options: t.Optional[t.Dict[str, t.Any]] = None,
    ) -> None:
        """Load a filesystem from a provider and kwargs.

        Args:
            uri: The filesystem URI.
            options: The filesystem provider kwargs.
        """
        if uri is None:
            raise ValueError("No filesystem URI provided")
        if isinstance(uri, Path):
            uri = uri.resolve().as_uri()
        proto = get_protocol(uri)

        self.uri = uri
        self.protocol = proto
        self.options = options or {}

    @cached_property
    def wrapped(self) -> fsspec.AbstractFileSystem:
        """Lazy handle to the filesystem."""
        from fsspec.implementations.dirfs import DirFileSystem

        options = self.options.copy()
        options.setdefault("auto_mkdir", True)
        return DirFileSystem(
            path=posixpath.join(strip_protocol(self.uri), "x")[:-1],
            fs=fsspec.filesystem(self.protocol, **options),
            auto_mkdir=True,
        )

    def __getattr__(self, name: str) -> t.Any:
        """Proxy attributes to the filesystem when not found."""
        return getattr(self.wrapped, name)

    def open(self, path: PathLike, mode: str = "r", **kwargs: t.Any) -> t.Any:
        """Open a file from the filesystem.

        Args:
            path: The path to the file.
            mode: The file mode.
            kwargs: Additional kwargs.

        Returns:
            The file handle.
        """
        return self.wrapped.open(path, mode, **kwargs)


def get_filesystem_adapter(
    settings: "FilesystemSettings", root: t.Optional[Path] = None
) -> M.Result[FilesystemAdapter, Exception]:
    """Create a filesystem adapter from settings.

    Args:
        settings: The filesystem settings.
        root: The root path to resolve the settings URI against. Usually the project root.

    Returns:
        The filesystem.
    """
    try:
        uri = settings.uri
        proto = get_protocol(uri)
        root_proto = "file"
        if root and proto == root_proto:
            uri = uri.replace(f"{root_proto}://", "")
            if not Path(uri).is_absolute():
                uri = root.resolve().joinpath(uri).as_uri()
        return M.ok(FilesystemAdapter(uri, settings.options))
    except Exception as e:
        return M.error(e)


__all__ = ["get_filesystem_adapter", "FilesystemAdapter"]
