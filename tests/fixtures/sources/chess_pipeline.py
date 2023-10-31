from functools import partial

from chess import source

__CDF_SOURCE__ = dict(
    chess_player_data_discrete=partial(
        source,
        ["magnuscarlsen", "vincentkeymer", "dommarajugukesh", "rpragchess"],
        start_month="2022/11",
        end_month="2022/12",
    ),
    chess_player_data=partial(
        source, ["magnuscarlsen", "vincentkeymer", "dommarajugukesh", "rpragchess"]
    ),
)
