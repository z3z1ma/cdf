import typing as t
from threading import Lock

import pydantic

from cdf.core.specification.base import InstallableRequirements, WorkspaceComponent


class NotebookSpecification(WorkspaceComponent, InstallableRequirements):
    """A sink specification."""

    storage_path: t.Optional[str] = None
    """The path to write the output notebook to for long term storage. 

    Uses the configured Project fs provider. This may be gcs, s3, etc.

    This is a format string which will be formatted with the following variables:
    - name: The name of the notebook.
    - date: The current date.
    - timestamp: An ISO formatted timestamp.
    - epoch: The current epoch time.
    - params: A dict of the resolved parameters passed to the notebook.
    """

    parameters: t.Dict[str, t.Any] = {}
    """Parameters to pass to the notebook when running."""
    gc_duration: int = 86400 * 3
    """The duration in seconds to keep the locally rendered notebook in the `_rendered` folder.

    Rendered notebooks are written to the `_rendered` folder of the notebook's parent directory.
    That folder is not intended to be a permanent storage location. This setting controls how long
    rendered notebooks are kept before being garbage collected. The default is 3 days. Set to 0 to
    clean up immediately after execution. Set to -1 to never clean up.
    """

    _folder: str = "notebooks"
    """The folder where notebooks are stored."""
    _extension: str = "ipynb"
    """The default extension for notebooks."""

    _lock: Lock = pydantic.PrivateAttr(default_factory=Lock)
    """A lock to ensure the notebook is thread safe."""


__all__ = ["NotebookSpecification"]
