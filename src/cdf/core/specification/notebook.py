import typing as t
from threading import Lock

import pydantic

from cdf.core.specification.base import InstallableRequirements, WorkspaceComponent


class NotebookSpecification(WorkspaceComponent, InstallableRequirements):
    """A sink specification."""

    storage_path: t.Optional[str] = None
    """The path to write the output notebook to for long term storage. 

    Setting this implies the output should be stored. Storage uses the configured fs provider.

    This is a format string which will be formatted with the following variables:
    - name: The name of the notebook.
    - date: The current date.
    - timestamp: An ISO formatted timestamp.
    - epoch: The current epoch time.
    - params: A dict of the resolved parameters passed to the notebook.
    """

    parameters: t.Dict[str, t.Any] = {}
    """Parameters to pass to the notebook when running."""
    keep_local_rendered: bool = True
    """Whether to keep the rendered notebook locally after running.

    Rendered notebooks are written to the `_rendered` folder of the notebook's parent directory.
    Setting this to False will delete the rendered notebook after running. This is independent
    of the long term storage offered by `storage_path` configuration.
    """

    _folder: str = "notebooks"
    """The folder where notebooks are stored."""

    _lock: Lock = pydantic.PrivateAttr(default_factory=Lock)
    """A lock to ensure the notebook is thread safe."""


__all__ = ["NotebookSpecification"]
