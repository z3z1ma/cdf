import contextlib
import os
import runpy
import shutil
import tempfile
import typing as t
from pathlib import Path

from cdf.core.monads import Err, Ok, Result

PathLike = t.Union[str, Path]


def run(
    code: str, root: PathLike = ".", quiet=False
) -> Result[t.Dict[str, t.Any], Exception]:
    """Run code in a sandbox.

    Args:
        code (str): The code to run.
        root (PathLike, optional): The root directory. Defaults to ".".

    Returns:
        Result[t.Dict[str, t.Any], Exception]: The result of the code execution.
    """
    try:
        with tempfile.TemporaryDirectory() as tmpdir:
            root_settings, temp_settings = Path(root) / ".dlt", Path(tmpdir) / ".dlt"
            if root_settings.exists():
                shutil.copytree(root_settings, temp_settings)
            f = Path(tmpdir) / "__main__.py"
            f.write_text(code)
            if quiet:
                with open(os.devnull, "w") as ignore, contextlib.redirect_stdout(
                    ignore
                ), contextlib.redirect_stderr(ignore):
                    exports = runpy.run_path(tmpdir, run_name="__main__")
            else:
                exports = runpy.run_path(tmpdir, run_name="__main__")
        return Ok(exports)
    except Exception as e:
        return Err(e)
