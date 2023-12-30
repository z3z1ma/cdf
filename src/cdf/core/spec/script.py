"""The spec classes for continuous data framework scripts."""
import typing as t

import cdf.core.constants as c
from cdf.core.spec.base import ComponentSpecification, Packageable, Schedulable

if t.TYPE_CHECKING:
    from cdf.core.workspace import Workspace


class ScriptInterface(t.Protocol):
    def __call__(
        self,
        workspace: "Workspace",
        last_execution_time: str | None = None,
        /,
        **kwargs: t.Any,
    ) -> int:
        ...


class ScriptSpecification(ComponentSpecification, Packageable, Schedulable):
    """A script specification."""

    _key = c.SCRIPTS

    @property
    def script(self) -> ScriptInterface:
        """The script function."""
        return self._main

    def __call__(self, workspace: "Workspace", **kwargs) -> int:
        """Call the script.

        Args:
            workspace: The workspace.
            **kwargs: The keyword arguments.

        Returns:
            int: The exit code.
        """
        return self.script(workspace, **kwargs)


__all__ = ["ScriptSpecification", "ScriptInterface"]
