import inspect
import typing as t

from .base import Entrypoint

if t.TYPE_CHECKING:
    from dlt.common.destination import Destination as DltDestination
    from dlt.common.pipeline import LoadInfo
    from dlt.pipeline.pipeline import Pipeline as DltPipeline


DataPipelineProto = t.Tuple[
    "DltPipeline",
    t.Union[
        t.Callable[..., "LoadInfo"],
        t.Callable[..., t.Iterator["LoadInfo"]],
    ],  # run
    t.List[t.Callable[..., t.Optional[t.Union[bool, t.Tuple[bool, str]]]]],  # tests
]


class DataPipeline(
    Entrypoint[DataPipelineProto],
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
        tpl = "{nr}. {message} ({state})"
        for nr, test in enumerate(tests, 1):
            result_struct = test()
            if isinstance(result_struct, bool) or result_struct is None:
                result, reason = result_struct, "No message"
            elif isinstance(result_struct, tuple):
                result, reason = result_struct
            else:
                raise ValueError(f"Invalid return type for test: {result_struct}")

            if result is True:
                print(tpl.format(nr=nr, state="PASS", message=reason))
            elif result is False:
                raise ValueError(tpl.format(nr=nr, state="FAIL", message=reason))
            elif result is None:
                print(tpl.format(nr=nr, state="SKIP", message=reason))
            else:
                raise ValueError(f"Invalid return value for test: {result}")
