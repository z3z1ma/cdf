import inspect
import typing as t

import pydantic

from .base import Entrypoint, _get_bind_func, _unwrap_entrypoint

if t.TYPE_CHECKING:
    from dlt.common.destination import Destination as DltDestination
    from dlt.common.pipeline import LoadInfo
    from dlt.pipeline.pipeline import Pipeline as DltPipeline


class DataPipeline(
    Entrypoint[
        t.Tuple[
            "DltPipeline",
            t.Union[
                t.Callable[..., "LoadInfo"],
                t.Callable[..., t.Iterator["LoadInfo"]],
            ],
            t.List[t.Callable[..., None]],
        ]
    ],
    frozen=True,
):
    """A data pipeline which loads data from a source to a destination."""

    def __call__(self, *args: t.Any, **kwargs: t.Any) -> t.List["LoadInfo"]:
        """Run the data pipeline"""
        _, runner, _ = self.main(*args, **kwargs)
        if inspect.isgeneratorfunction(runner):
            return list(runner())
        return [t.cast("LoadInfo", runner())]

    def get_schemas(self, destination: t.Optional["DltDestination"] = None):
        """Get the schemas for the pipeline."""
        pipeline, _, _ = self.main()
        pipeline.sync_destination(destination=destination)
        return pipeline.schemas

    def run_tests(self) -> None:
        """Run the integration test for the pipeline."""
        _, _, tests = self.main()
        if not tests:
            raise ValueError("No tests found for pipeline")
        for test in tests:
            test()
