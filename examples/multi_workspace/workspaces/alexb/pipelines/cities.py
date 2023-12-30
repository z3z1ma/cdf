import dlt
import requests


@dlt.resource(write_disposition="replace")
def us_cities():
    yield requests.get(
        "https://raw.githubusercontent.com/millbj92/US-Zip-Codes-JSON/master/USCities.json"
    ).json()


@dlt.source
def us_zip_codes():
    """Get zip code data"""
    return [us_cities]
