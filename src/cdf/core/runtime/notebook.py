"""The runtime notebook module is responsible for executing notebooks from notebook specifications.

It performs the following functions:
- Executes the notebook.
- Writes the output to a designated location in a storage provider.
- Cleans up the rendered notebook if required.
"""

import sys
import time
import typing as t
from datetime import date, datetime

import fsspec
import papermill

import cdf.core.logger as logger
from cdf.core.specification import NotebookSpecification
from cdf.types import M

if t.TYPE_CHECKING:
    from nbformat import NotebookNode


def execute_notebook_specification(
    spec: NotebookSpecification,
    storage: t.Optional[fsspec.AbstractFileSystem] = None,
    **params: t.Any,
) -> M.Result["NotebookNode", Exception]:
    """Execute a notebook specification.

    Args:
        spec: The notebook specification to execute.
        storage: The filesystem to use for persisting the output.
        **params: The parameters to pass to the notebook. Overrides the notebook spec parameters.
    """
    origpath = sys.path[:]
    sys.path = [
        str(spec.workspace_path),
        *sys.path,
        str(spec.workspace_path.parent),
    ]
    try:
        merged_params = {**spec.parameters, **params}
        output = spec.path.parent.joinpath(
            "_rendered", f"{spec.name}.{int(time.monotonic())}.ipynb"
        )
        output.parent.mkdir(parents=True, exist_ok=True)
        with spec._lock:
            rv: "NotebookNode" = papermill.execute_notebook(
                spec.path,
                output,
                merged_params,
                cwd=spec.workspace_path,
            )
        logger.info(
            f"Successfully ran notebook {spec.path} with params {merged_params} staged into {path}"
        )
        if storage and spec.storage_path:
            storage_path = spec.storage_path.format(
                name=spec.name,
                date=date.today(),
                timestamp=datetime.now().isoformat(),
                epoch=time.time(),
                params=merged_params,
            )
            logger.info(
                f"Persisting output to {storage_path} with fs protocol {storage.protocol}"
            )
            storage.put_file(output, storage_path)
        if not spec.keep_local_rendered:
            output.unlink()
        return M.ok(rv)
    except Exception as e:
        logger.error(f"Error running notebook {spec.path}: {e}")
        return M.error(e)
    finally:
        sys.path = origpath
