import os
import typing as t

import cdf.core.constants as c


class EngineCredentials(t.NamedTuple):
    engine: str
    credentials: t.Dict[str, str] | str


DestinationSpec = t.Dict[str, EngineCredentials]


def index_destinations(environment: t.Dict[str, str] | None = None) -> DestinationSpec:
    """Index destinations from the environment based on a standard convention.

    Notes:
        Convention is as follows:

        CDF__<DESTINATION_NAME>__ENGINE=<ENGINE_NAME>
        CDF__<DESTINATION_NAME>__CREDENTIALS=<NATIVE_VALUE>
        CDF__<DESTINATION_NAME>__CREDENTIALS__<KEY>=<VALUE>

    Returns:
        A dict of destination names to tuples of engine names and credentials.
    """
    destinations: DestinationSpec = {
        "default": EngineCredentials("duckdb", "duckdb:///cdf.db"),
    }
    env = environment or os.environ.copy()
    env_creds = {}
    for k, v in env.items():
        match = c.DEST_ENGINE_PAT.match(k)
        if match:
            dest_name = match.group("dest_name")
            env_creds.setdefault(dest_name.lower(), {})["engine"] = v
            continue
        match = c.DEST_NATIVECRED_PAT.match(k)
        if match:
            dest_name = match.group("dest_name")
            env_creds.setdefault(dest_name.lower(), {})["credentials"] = v
            continue
        match = c.DEST_CRED_PAT.match(k)
        if match:
            dest_name = match.group("dest_name")
            frag = env_creds.setdefault(dest_name.lower(), {})
            if isinstance(frag.get("credentials"), str):
                continue  # Prioritize native creds
            frag.setdefault("credentials", {})[match.group("key").lower()] = v
    for dest, creds in env_creds.items():
        if "engine" not in creds or "credentials" not in creds:
            continue
        destinations[dest.lower()] = EngineCredentials(
            creds["engine"], creds["credentials"]
        )
    return destinations
