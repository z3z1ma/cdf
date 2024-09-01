import inspect
import typing as t

from .base import Entrypoint

if t.TYPE_CHECKING:
    from dlt.common.destination import Destination as DltDestination
    from dlt.common.pipeline import LoadInfo
    from dlt.pipeline.pipeline import Pipeline as DltPipeline

_GRN = "\033[32;1m"
_YLW = "\033[33;1m"
_RED = "\033[31;1m"
_CLR = "\033[0m"

TEST_RESULT_MAP = {
    None: f"{_YLW}SKIP{_CLR}",
    True: f"{_GRN}PASS{_CLR}",
    False: f"{_RED}FAIL{_CLR}",
}

DataPipelineProto = t.Tuple[
    "DltPipeline",
    t.Union[
        t.Callable[..., "LoadInfo"],
        t.Callable[..., t.Iterator["LoadInfo"]],
    ],  # run
    t.Sequence[t.Callable[..., t.Optional[t.Union[bool, t.Tuple[bool, str]]]]],  # tests
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

    run = __call__

    def unwrap(self) -> "DltPipeline":
        """Get the dlt pipeline object."""
        pipeline, _, _ = self.main()
        return pipeline

    def get_schemas(self, destination: t.Optional["DltDestination"] = None):
        """Get the schemas for the pipeline."""
        pipeline = self.unwrap()
        pipeline.sync_destination(destination=destination)
        return pipeline.schemas

    def run_tests(self) -> None:
        """Run the integration test for the pipeline."""
        _, _, tests = self.main()
        if not tests:
            raise ValueError("No tests found for pipeline")
        tpl = "[{nr}/{tot}] {message} ({state})"
        tot = len(tests)
        for nr, test in enumerate(tests, 1):
            result_struct = test()
            if isinstance(result_struct, bool) or result_struct is None:
                result, reason = result_struct, "No message"
            elif isinstance(result_struct, tuple):
                result, reason = result_struct
            else:
                raise ValueError(
                    f"Invalid return type `{type(result_struct)}`, expected none, bool, or tuple(bool, str)"
                )
            if result not in TEST_RESULT_MAP:
                raise ValueError(f"Invalid return status for test: `{result}`")
            print(
                tpl.format(
                    nr=nr, tot=tot, state=TEST_RESULT_MAP[result], message=reason
                )
            )
