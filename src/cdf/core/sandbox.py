"""Sandbox for executing cdf scripts with a temporary environment."""
import contextlib
import os
import runpy
import tempfile
import typing as t
from pathlib import Path

from dlt.common.configuration.container import Container
from dlt.common.configuration.providers import (
    ConfigTomlProvider,
    EnvironProvider,
    SecretsTomlProvider,
)
from dlt.common.configuration.specs.config_providers_context import (
    ConfigProvidersContext,
)

import cdf.core.exceptions as ex
from cdf.core.monads import Err, Ok, Result

PathLike = t.Union[str, Path]


def run(
    code: str, root: PathLike = ".", quiet=False
) -> Result[t.Dict[str, t.Any], ex.CDFError]:
    """Run code in a sandbox.

    Args:
        code (str): The code to run.
        root (PathLike, optional): The root directory. Defaults to ".".

    Returns:
        Result[t.Dict[str, t.Any], Exception]: The result of the code execution.
    """
    root = Path(root)
    try:
        C = ConfigProvidersContext()
        C.providers = [
            EnvironProvider(),
            SecretsTomlProvider(os.path.join(root, ".dlt")),
            ConfigTomlProvider(os.path.join(root, ".dlt")),
        ]
        runkwargs: t.Dict[str, t.Any] = dict(
            run_name="__main__", init_globals={"__root__": str(root.resolve())}
        )
        with tempfile.TemporaryDirectory() as tmpdir, Container().injectable_context(C):
            f = Path(tmpdir) / "__main__.py"
            f.write_text(code)
            if quiet:
                with open(os.devnull, "w") as ignore, contextlib.redirect_stdout(
                    ignore
                ), contextlib.redirect_stderr(ignore):
                    exports = runpy.run_path(tmpdir, **runkwargs)
            else:
                exports = runpy.run_path(tmpdir, **runkwargs)
        return Ok(exports)
    except Exception as e:
        return Err(ex.CDFError(f"Error running code: {e}", e))
