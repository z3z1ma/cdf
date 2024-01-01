"""Dota2 is a Massive Online Battle Arena game based on Warcraft."""
import dlt
import dlt.sources.helpers.requests as requests


@dlt.resource(write_disposition="merge", primary_key="account_id")
def pro_players():
    """Get list of pro players"""
    yield requests.get("https://api.opendota.com/api/proPlayers").json()


@dlt.resource(write_disposition="merge", primary_key="match_id")
def pro_matches():
    """Get list of pro matches"""
    yield requests.get("https://api.opendota.com/api/proMatches").json()


@dlt.resource(write_disposition="replace")
def distribution():
    """Distributions of MMR data by bracket and country"""
    yield requests.get("https://api.opendota.com/api/distributions").json()


@dlt.resource(write_disposition="replace")
def rankings():
    """Top players by hero"""
    yield requests.get("https://api.opendota.com/api/rankings").json()


@dlt.resource(write_disposition="replace")
def benchmarks():
    """Benchmarks of average stat values for a hero"""
    yield requests.get("https://api.opendota.com/api/benchmarks").json()


@dlt.resource(write_disposition="replace")
def heroes():
    """Get hero data"""
    yield requests.get("https://api.opendota.com/api/heroes").json()


@dlt.resource(write_disposition="replace")
def hero_stats():
    """Get stats about hero performance in recent matches"""
    yield requests.get("https://api.opendota.com/api/heroStats").json()


@dlt.resource(write_disposition="replace")
def leagues():
    """Get league data"""
    yield requests.get("https://api.opendota.com/api/leagues").json()


@dlt.resource(write_disposition="replace")
def teams():
    """Get team data"""
    yield requests.get("https://api.opendota.com/api/teams").json()


@dlt.resource(write_disposition="replace")
def constants():
    """Download all constants from odota/dotaconstants"""

    for table in (
        "game_mode",
        "item_colors",
        "lobby_type",
        "order_types",
        "patch",
        "permanent_buffs",
        "player_colors",
        "skillshots",
        "xp_level",
    ):
        raw_data = requests.get(
            f"https://raw.githubusercontent.com/odota/dotaconstants/master/json/{table}.json"
        ).json()

        if table in ("game_mode", "lobby_type"):
            data = list(raw_data.values())
        elif table in (
            "item_colors",
            "order_types",
            "permanent_buffs",
            "player_colors",
            "skillshots",
        ):
            data = [{"id": k, "value": v} for k, v in raw_data.items()]
        elif table == "xp_level":
            data = [{"level": i, "xp": v} for i, v in enumerate(raw_data)]
        else:
            data = raw_data

        yield dlt.mark.with_table_name(data, table)


@dlt.source
def dota2_stats():
    """This source contains Dota 2 data from OpenDota API and repository"""
    return (
        pro_players(),
        pro_matches(),
        distribution(),
        rankings(),
        benchmarks(),
        heroes(),
        hero_stats(),
        leagues(),
        teams(),
        constants(),
    )
