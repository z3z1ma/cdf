from functools import partial

from pipelines.chess import source

from cdf import pipeline_spec

__CDF_PIPELINES__ = [
    pipeline_spec(
        "chess_player_data_discrete",
        pipeline_gen=partial(
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
    pipeline_spec(
        "chess_player_data",
        pipeline_gen=partial(
            source,
            ["magnuscarlsen", "vincentkeymer", "dommarajugukesh", "rpragchess"],
        ),
        version=1,
        owners=("qa-team"),
        description="A source that extracts chess player data.",
        tags=("api", "live", "test"),
    ),
]
