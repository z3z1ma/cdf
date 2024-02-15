import os
import runpy
import tempfile
import typing as t
from pathlib import Path

from cdf.core.monads import Err, Ok, Result

PathLike = t.Union[str, Path]

ENV_PROJECT_DIR = "DLT_PROJECT_DIR"
"""Config injection support leveraging workspace-specific .dlt/config.toml and .dlt/secrets.toml files"""


def run(code: str, root: PathLike = ".") -> Result[t.Dict[str, t.Any], Exception]:
    """Run code in a sandbox.

    Args:
        code (str): The code to run.
        root (PathLike, optional): The root directory. Defaults to ".".

    Returns:
        Result[t.Dict[str, t.Any], Exception]: The result of the code execution.
    """
    try:
        origprojdir = os.environ.get(ENV_PROJECT_DIR)
        os.environ[ENV_PROJECT_DIR] = str(root)
        with tempfile.TemporaryDirectory() as tmpdir:
            f = Path(tmpdir) / "__main__.py"
            f.write_text(code)
            exports = runpy.run_path(tmpdir, run_name="__main__")
        return Ok(exports)
    except Exception as e:
        return Err(e)
    finally:
        if origprojdir is not None:
            os.environ[ENV_PROJECT_DIR] = origprojdir
        else:
            del os.environ[ENV_PROJECT_DIR]
