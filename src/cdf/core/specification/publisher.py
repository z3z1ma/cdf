import typing as t

from cdf.core.specification.base import PythonScript, Schedulable


class PublisherSpecification(PythonScript, Schedulable):
    """A publisher specification."""

    depends_on: t.List = []
    """The dependencies of the publisher expressed as fully qualified names of SQLMesh tables."""

    _folder = "publishers"
    """The folder where publisher scripts are stored."""


__all__ = ["PublisherSpecification"]
