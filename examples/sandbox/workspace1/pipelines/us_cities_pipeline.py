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

from .test1.chore import foo


@dlt.resource(write_disposition="replace", standalone=True)
def us_cities():
    yield requests.get(
        "https://raw.githubusercontent.com/millbj92/US-Zip-Codes-JSON/master/USCities.json"
    ).json()


if cdf.execute():
    resource = us_cities()

    pipeline = cdf.pipeline()

    load_info = pipeline.run(resource, table_name="cities")

    print(load_info)
