"""The runtime script module is responsible for executing scripts from script specifications.

It performs the following functions:
- Executes the script.
- Optionally captures stdout and returns it as a string.
"""

import io
import typing as t
from contextlib import nullcontext, redirect_stdout

import cdf.core.logger as logger
from cdf.core.runtime.common import with_activate_project
from cdf.core.specification import ScriptSpecification
from cdf.types import M


@t.overload
def execute_script_specification(
    spec: ScriptSpecification,
    capture_stdout: bool = False,
) -> M.Result[t.Dict[str, t.Any], Exception]: ...


@t.overload
def execute_script_specification(
    spec: ScriptSpecification,
    capture_stdout: bool = True,
) -> M.Result[str, Exception]: ...


@with_activate_project
def execute_script_specification(
    spec: ScriptSpecification,
    capture_stdout: bool = False,
) -> t.Union[M.Result[t.Dict[str, t.Any], Exception], M.Result[str, Exception]]:
    """Execute a script specification.

    Args:
        spec: The script specification to execute.
        capture_stdout: Whether to capture stdout and return it. False returns an empty string.
    """
    try:
        buf = io.StringIO()
        maybe_redirect = redirect_stdout(buf) if capture_stdout else nullcontext()
        logger.info(f"Running script {spec.path}")
        with maybe_redirect:
            exports = spec()
        return M.ok(buf.getvalue() if capture_stdout else exports)  # type: ignore
    except Exception as e:
        logger.error(f"Error running script {spec.path}: {e}")
        return M.error(e)


__all__ = ["execute_script_specification"]
