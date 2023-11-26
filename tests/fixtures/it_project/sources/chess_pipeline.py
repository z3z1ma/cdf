from functools import partial

from sources.chess import source

from cdf import source_spec

__CDF_SOURCE__ = {
    "chess_player_data_discrete": source_spec(
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
    "chess_player_data": source_spec(
        factory=partial(
            source,
            ["magnuscarlsen", "vincentkeymer", "dommarajugukesh", "rpragchess"],
        ),
        version=1,
        owners=("qa-team"),
        description="A source that extracts chess player data.",
        tags=("api", "live", "test"),
    ),
}
