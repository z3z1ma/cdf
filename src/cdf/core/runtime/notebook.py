"""The runtime notebook module is responsible for executing notebooks from notebook specifications.

It performs the following functions:
- Executes the notebook.
- Writes the output to a designated location in a storage provider.
- Cleans up the rendered notebook if required.
"""

import re
import sys
import time
import typing as t
from contextlib import nullcontext
from datetime import date, datetime
from pathlib import Path

import papermill

import cdf.core.logger as logger
from cdf.core.runtime.common import with_activate_project
from cdf.core.specification import NotebookSpecification
from cdf.types import M

if t.TYPE_CHECKING:
    from nbformat import NotebookNode


@with_activate_project
def execute_notebook_specification(
    spec: NotebookSpecification,
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
        str(spec.root_path),
        *sys.path,
        str(spec.root_path.parent),
    ]
    try:
        merged_params = {**spec.parameters, **params}
        output = spec.path.parent.joinpath(
            "_rendered", f"{spec.name}.{int(time.time())}.ipynb"
        )
        output.parent.mkdir(parents=True, exist_ok=True)
        if spec.has_workspace_association:
            workspace_context = spec.workspace.inject_configuration()
        else:
            workspace_context = nullcontext()
        with spec._lock, workspace_context:
            rv: "NotebookNode" = papermill.execute_notebook(
                spec.path,
                output,
                merged_params,
                cwd=spec.root_path,
            )
        logger.info(
            f"Successfully ran notebook {spec.path} with params {merged_params} rendered into {output}"
        )
        storage = spec.workspace.fs_adapter
        if storage and spec.storage_path:
            storage_path = spec.storage_path.format(
                name=spec.name,
                date=date.today(),
                timestamp=datetime.now().isoformat(timespec="seconds"),
                epoch=time.time(),
                params=merged_params,
                ext=spec.path.suffix,
            )
            logger.info(f"Persisting output to {storage_path} with {storage}")
            storage.put_file(output, storage_path)
        if spec.gc_duration >= 0:
            _gc_rendered(output.parent, spec.name, spec.gc_duration)
        return M.ok(rv)
    except Exception as e:
        logger.error(f"Error running notebook {spec.path}: {e}")
        return M.error(e)
    finally:
        sys.path = origpath


def _gc_rendered(path: Path, name: str, max_ttl: int) -> None:
    """Garbage collect rendered notebooks."""
    now = time.time()
    for nb in path.glob(f"{name}.*.ipynb"):
        ts_str = re.search(r"\d{10}", nb.stem)
        if ts_str:
            ts = int(ts_str.group())
            if now - ts > max_ttl:
                nb.unlink()


__all__ = ["execute_notebook_specification"]
