import shlex
import sys
from pathlib import Path

import shellingham

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
        shell, _ = shellingham.detect_shell()
    except shellingham.ShellDetectionFailure:
        shell = ""
    if shell == "fish":
        command, filename = "source", "activate.fish"
    elif shell == "nu":
        command, filename = "overlay use", "activate.nu"
    elif shell == "csh":
        command, filename = "source", "activate.csh"
    elif shell in ["powershell", "pwsh"]:
        command, filename = ".", "Activate.ps1"
    elif shell == "cmd":
        command, filename = ".", "activate.bat"
    else:
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
