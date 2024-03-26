"""A wrapper around a CDF project."""

import typing as t

import fsspec

from cdf.core.configuration import load_config
from cdf.core.filesystem import load_filesystem
from cdf.types import PathLike


class Project:
    def __init__(self, root: PathLike):
        self.configuration = load_config(root).unwrap()
        self._workspace: t.Optional[str] = None

    def set_active_workspace(self, workspace: t.Optional[str] = None) -> "Project":
        self._workspace = workspace
        return self

    def clear_active_workspace(self) -> "Project":
        self._workspace = None
        return self

    def __enter__(self, workspace: t.Optional[str] = None) -> "Project":
        return self.set_active_workspace(workspace)

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        self.clear_active_workspace()
        if exc_type is not None:
            ...

    @property
    def filesystem(self) -> fsspec.AbstractFileSystem:
        fs = self.configuration[_to_key("filesystem", self._workspace)]
        return load_filesystem(fs.provider, **fs.options.to_dict())

    @property
    def feature_flag_provider(self) -> None:
        ff = self.configuration[_to_key("feature_flags", self._workspace)]
        print(ff.provider)


def _to_key(
    key: str, workspace: t.Optional[str] = None
) -> t.Union[str, t.Tuple[str, str]]:
    return key if workspace is None else (workspace, key)
