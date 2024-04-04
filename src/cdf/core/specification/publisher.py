import typing as t

import pydantic

from cdf.core.specification.base import PythonScript, Schedulable


class PublisherSpecification(PythonScript, Schedulable):
    """A publisher specification."""

    depends_on: t.List = []
    """The dependencies of the publisher expressed as fully qualified names of SQLMesh tables."""

    _folder = "publishers"
    """The folder where publisher scripts are stored."""

    @pydantic.model_validator(mode="before")
    @classmethod
    def _clean_path_and_name(cls, config: t.Any) -> t.Any:
        if config["name"].endswith(".py"):
            if "path" not in config:
                config["path"] = config["name"]
            config["name"] = config["name"][:-3]
        else:
            if "path" not in config:
                if config["name"].endswith("_publisher"):
                    config["path"] = f"{config['name']}.py"
                else:
                    config["path"] = f"{config['name']}_publisher.py"
        return config


__all__ = ["PublisherSpecification"]
