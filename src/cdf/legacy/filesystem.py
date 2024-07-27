"""A central interface for filesystems thinly wrapping fsspec."""

import posixpath
import typing as t
from pathlib import Path

import dlt
import fsspec
from dlt.common.configuration import with_config
from fsspec.core import strip_protocol
from fsspec.implementations.dirfs import DirFileSystem
from fsspec.utils import get_protocol

from cdf.types import PathLike


# TODO: Add UPath integration...
class FilesystemAdapter:
    """Wraps an fsspec filesystem.

    The filesystem is lazily loaded. Certain methods are intercepted to include cdf-specific logic. Helper
    methods are provided for specific operations.
    """

    @with_config(sections=("filesystem",))
    def __init__(
        self,
        uri: PathLike = dlt.config.value,
        root: t.Optional[PathLike] = None,
        options: t.Optional[t.Dict[str, t.Any]] = None,
    ) -> None:
        """Load a filesystem from a provider and kwargs.

        Args:
            uri: The filesystem URI.
            options: The filesystem provider kwargs.
        """
        uri = _resolve_local_uri(uri, root)
        if isinstance(uri, Path):
            uri = uri.resolve().as_uri()
        options = options or {}
        options.setdefault("auto_mkdir", True)
        CdfFs = type("CdfFs", (DirFileSystem,), {"protocol": "cdf"})
        self.wrapped = CdfFs(
            path=posixpath.join(strip_protocol(uri), "x")[:-1],
            fs=fsspec.filesystem(get_protocol(uri), **options),
            auto_mkdir=True,
        )
        self.uri = uri
        self.mapper = self.wrapped.get_mapper()

    def __repr__(self) -> str:
        return f"{type(self).__name__}(uri={self.uri!r})"

    def __str__(self) -> str:
        return self.uri

    def __getattr__(self, name: str) -> t.Any:
        """Proxy attribute access to the wrapped filesystem."""
        return getattr(self.wrapped, name)

    def __getitem__(self, value: str) -> t.Any:
        """Get a path from the filesystem."""
        return self.mapper[value]

    def __setitem__(self, key: str, value: t.Any) -> None:
        """Set a path in the filesystem."""
        self.mapper[key] = value

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


def _resolve_local_uri(uri: PathLike, root: t.Optional[PathLike] = None) -> PathLike:
    """Resolve a local URI to an absolute path. If the URI is already absolute, it is returned as-is.

    URIs with protocols other than "file" are returned as-is.

    Args:
        uri: The URI to resolve.
        root: The root path to use.

    Returns:
        The resolved URI.
    """
    uri_str = str(uri)
    proto = get_protocol(uri_str)
    root_proto = "file"
    if root and proto == root_proto:
        uri_str = uri_str.replace(f"{root_proto}://", "")
        if not Path(uri_str).is_absolute():
            uri = Path(root, uri_str).resolve().as_uri()
    return uri


__all__ = ["FilesystemAdapter"]
