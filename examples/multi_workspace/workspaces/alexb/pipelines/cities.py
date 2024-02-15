"""
PIPELINE (
    name us_cities,
    description 'Load US cities',
    cron '0 0 * * *',
);
"""
import dlt
import requests


@dlt.resource(write_disposition="replace", standalone=True)
def us_cities():
    yield requests.get(
        "https://raw.githubusercontent.com/millbj92/US-Zip-Codes-JSON/master/USCities.json"
    ).json()


resource = us_cities()

pipeline = dlt.pipeline("cities")

load_info = pipeline.run(resource, destination="duckdb", table_name="cities")

print(load_info)
