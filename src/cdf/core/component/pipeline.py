import typing as t

import pydantic

from .base import Entrypoint, _get_bind_func, _unwrap_entrypoint

if t.TYPE_CHECKING:
    from dlt.common.destination import Destination as DltDestination
    from dlt.common.pipeline import LoadInfo
    from dlt.pipeline.pipeline import Pipeline as DltPipeline


class DataPipeline(
    Entrypoint[t.Tuple["DltPipeline", t.Callable[..., t.Optional["LoadInfo"]]]],
    frozen=True,
):
    """A data pipeline which loads data from a source to a destination."""

    integration_test: t.Optional[t.Callable[..., bool]] = None
    """A function to test the pipeline in an integration environment"""

    @pydantic.field_validator("integration_test", mode="before")
    @classmethod
    def _bind_ancillary(cls, value: t.Any, info: pydantic.ValidationInfo) -> t.Any:
        """Bind the active workspace to the ancillary functions."""
        return _get_bind_func(info)(_unwrap_entrypoint(value))

    def __call__(self, *args: t.Any, **kwargs: t.Any) -> t.Optional["LoadInfo"]:
        """Run the data pipeline"""
        _, runner = self.main(*args, **kwargs)
        return runner()

    def get_schemas(self, destination: t.Optional["DltDestination"] = None):
        """Get the schemas for the pipeline."""
        pipeline, _ = self.main()
        pipeline.sync_destination(destination=destination)
        return pipeline.schemas
