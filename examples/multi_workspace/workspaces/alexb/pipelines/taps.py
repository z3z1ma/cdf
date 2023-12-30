import json
import subprocess

import dlt


@dlt.resource
def singer_taps():
    json_data = subprocess.check_output(
        "cat $(fd --glob '*.json' ~/code_projects/personal/hub/singer/taps/) | jq -c",
        shell=True,
    )
    for line in json_data.splitlines():
        yield json.loads(line)


@dlt.source
def meltano_hub_repo():
    return [singer_taps]
