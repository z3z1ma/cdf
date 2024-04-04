import sys
import time
import typing as t
from pathlib import Path
from threading import Lock

import papermill
import pydantic

import cdf.core.logger as logger
from cdf.core.specification.base import InstallableRequirements, WorkspaceComponent


class NotebookSpecification(WorkspaceComponent, InstallableRequirements):
    """A sink specification."""

    write_path: t.Optional[str] = None
    """The path to write the output notebook to. Leverages the configured filesystem provider.

    This is a format string which will be formatted with the following variables:
    - name: The name of the notebook.
    - date: The current date.
    - timestamp: An ISO formatted timestamp.
    - epoch: The current epoch time.
    - params: A dict of the parameters passed to the notebook.
    """

    parameters: t.Dict[str, t.Any] = {}
    """Parameters to pass to the notebook when running."""
    keep_local_rendered: bool = True
    """Whether to keep the rendered notebook locally after running."""

    _folder: str = "notebooks"
    """The folder where notebooks are stored."""

    _lock: Lock = pydantic.PrivateAttr(default_factory=Lock)
    """A lock to ensure the notebook is thread safe."""

    def _run(self, **params: t.Any) -> t.Tuple[Path, t.Dict[str, t.Any]]:
        """Run the notebook and return the path to the output and the input parameters."""
        origpath = sys.path[:]
        sys.path = [
            str(self.workspace_path),
            *sys.path,
            str(self.workspace_path.parent),
        ]
        params_ = self.parameters.copy()
        params_.update(params)
        try:
            output = self.path.parent.joinpath(
                "_rendered", f"{self.name}.{int(time.monotonic())}.ipynb"
            )
            output.parent.mkdir(parents=True, exist_ok=True)
            with self._lock:
                papermill.execute_notebook(
                    self.path,
                    output,
                    params_,
                    cwd=self.workspace_path,
                )
            return output, params_
        except Exception as e:
            logger.error(f"Error running notebook {self.path}: {e}")
            raise
        finally:
            sys.path = origpath
