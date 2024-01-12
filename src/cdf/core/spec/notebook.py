"""The spec classes for continuous data framework scripts."""
import time
import typing as t

import papermill

import cdf.core.constants as c
import cdf.core.logger as logger
from cdf.core.spec.base import ComponentSpecification, Packageable, Schedulable

if t.TYPE_CHECKING:
    from nbformat import NotebookNode

    from cdf.core.workspace import Workspace


class NotebookSpecification(ComponentSpecification, Packageable, Schedulable):
    """A notebook specification."""

    input_path: str
    """Relative path to the notebook starting from the notebook root."""
    output_path: str
    """Relative path to the output notebook starting from the notebook root."""
    parameters: t.Dict[str, t.Any] = {}
    """Parameters to pass to the notebook."""

    _key = c.NOTEBOOKS

    def __call__(self, ws: "Workspace", **params: t.Any) -> "NotebookNode":
        """
        Execute the notebook.

        Args:
            ws: The workspace which contains the notebook.
            **params: The parameters to pass to the notebook.
        """
        inp = ws.root / c.NOTEBOOKS / self.input_path.lstrip("/")
        out = ws.root / c.NOTEBOOKS / self.output_path.lstrip("/")
        if not inp.exists():
            raise FileNotFoundError(f"Notebook {self.input_path} does not exist.")
        out.parent.mkdir(parents=True, exist_ok=True)

        logger.info("Executing notebook %s @ %s", self.name, inp)
        notebookstart = time.perf_counter()
        nb: "NotebookNode" = papermill.execute_notebook(
            inp, out, self.parameters | params, cwd=ws.root
        )
        logger.debug("Notebook output: %s", nb)
        notebookend = time.perf_counter()
        logger.info(
            "Notebook execution completed in %.3f seconds", notebookend - notebookstart
        )

        return nb


__all__ = ["NotebookSpecification"]
