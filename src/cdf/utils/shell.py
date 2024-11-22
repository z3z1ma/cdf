# pyright: reportUnknownMemberType=false
"""Shell oriented utilities."""

import shlex
import sys
import typing as t
from pathlib import Path

import shellingham  # pyright: ignore[reportMissingTypeStubs]

__all__ = [
    "get_activate_command",
    "WINDOWS",
]

WINDOWS = sys.platform == "win32"


# Adapted from Poetry's source code
# https://github.com/python-poetry/poetry/blob/bf8fb712b3ad5a9584af93634ee24ea0fdab89bc/src/poetry/console/commands/env/activate.py#L38
def get_activate_command(root_path: Path) -> str:
    """Get the command to activate the virtual environment.

    Args:
        root_path (Path): The root path of the project.

    Returns:
        str: The command to activate the virtual environment.
    """
    try:
        shell, _ = t.cast(tuple[str, t.Any], shellingham.detect_shell())
    except shellingham.ShellDetectionFailure:
        shell = ""
    match shell:
        case "fish":
            command, filename = "source", "activate.fish"
        case "nu":
            command, filename = "overlay use", "activate.nu"
        case "csh":
            command, filename = "source", "activate.csh"
        case "powershell" | "pwsh":
            command, filename = ".", "Activate.ps1"
        case "cmd":
            command, filename = ".", "activate.bat"
        case _:
            command, filename = "source", "activate"
    if (activation_script := root_path / filename).exists():
        if WINDOWS:
            return f"{_quote(str(activation_script), shell)}"
        return f"{command} {_quote(str(activation_script), shell)}"
    return ""


def _quote(command: str, shell: str) -> str:
    """Quote a command for the given shell.

    Args:
        command (str): The command to quote.
        shell (str): The shell to quote the command for.

    Returns:
        str: The quoted command.
    """
    if WINDOWS:
        if shell == "cmd":
            return f'"{command}"'
        if shell in ["powershell", "pwsh"]:
            return f'& "{command}"'
    return shlex.quote(command)
