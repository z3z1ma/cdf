"""
PIPELINE (
    name us_cities,
    description 'Load US cities',
    cron '0 0 * * *',
);
"""

import dlt
import requests

import cdf

# A relative import from the workspace
from .test1.chore import foo


@dlt.resource(write_disposition="append", standalone=True)
def us_cities():
    """Load US cities"""
    foo()
    yield requests.get(
        "https://raw.githubusercontent.com/millbj92/US-Zip-Codes-JSON/master/USCities.json"
    ).json()


if cdf.execute():
    pipeline = cdf.pipeline()

    load_info = pipeline.run(us_cities(), table_name="cities")

    print(load_info)
