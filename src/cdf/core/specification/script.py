from cdf.core.specification.base import PythonScript, Schedulable


class ScriptSpecification(PythonScript, Schedulable):
    """A script specification."""

    _folder = "scripts"
    """The folder where generic scripts are stored."""


__all__ = ["ScriptSpecification"]
