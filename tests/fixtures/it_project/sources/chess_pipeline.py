from functools import partial

from chess import source

from cdf import CDFSourceWrapper

__CDF_SOURCE__ = dict(
    chess_player_data_discrete=CDFSourceWrapper(
        factory=partial(
            source,
            ["magnuscarlsen", "vincentkeymer", "dommarajugukesh", "rpragchess"],
            start_month="2022/11",
            end_month="2022/12",
        ),
        version=1,
        owners=("qa-team"),
        description="A source that extracts chess player data from a discrete period.",
        tags=("api", "live", "test"),
    ),
    chess_player_data=CDFSourceWrapper(
        factory=partial(
            source, ["magnuscarlsen", "vincentkeymer", "dommarajugukesh", "rpragchess"]
        ),
        version=1,
        owners=("qa-team"),
        description="A source that extracts chess player data.",
        tags=("api", "live", "test"),
        metrics={
            "count": lambda item, metric=0: metric + 1,
            "max_acv": lambda item, metric=0: max(item["acv"], metric),
        },
    ),
)
