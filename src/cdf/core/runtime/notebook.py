"""The runtime notebook module is responsible for executing notebooks from notebook specifications.

It performs the following functions:
- Executes the notebook.
- Writes the output to a designated location in an fs provider.
"""

import time
import typing as t
from datetime import date, datetime
from pathlib import Path

import fsspec

import cdf.core.logger as logger
from cdf.core.specification import NotebookSpecification
from cdf.types import M


def execute_notebook_specification(
    spec: NotebookSpecification,
    fs: t.Optional[fsspec.AbstractFileSystem] = None,
    **params: t.Any,
) -> M.Result[Path, Exception]:
    """Execute a notebook specification.

    Args:
        spec: The notebook specification to execute.
        fs: The filesystem to use for persisting the output.
        **params: The parameters to pass to the notebook.
    """
    try:
        path, resolved_params = spec._run(**params)
        logger.info(
            f"Successfully ran notebook {spec.path} with params {resolved_params} staged into {path}"
        )
        if fs and spec.write_path:
            storage_path = spec.write_path.format(
                name=spec.name,
                date=date.today(),
                timestamp=datetime.now().isoformat(),
                epoch=time.time(),
                params=resolved_params,
            )
            logger.info(
                f"Persisting output to {storage_path} with fs protocol {fs.protocol}"
            )
            fs.put_file(path, storage_path)
        if not spec.keep_local_rendered:
            path.unlink()
        return M.ok(path)
    except Exception as e:
        logger.error(f"Error running script {spec.path}: {e}")
        return M.error(e)
