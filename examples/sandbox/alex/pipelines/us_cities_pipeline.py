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
    foo()  # Call a function from a relative import
    yield requests.get(
        "https://raw.githubusercontent.com/millbj92/US-Zip-Codes-JSON/master/USCities.json"
    ).json()


if cdf.is_main(__name__):
    # Define a pipeline
    pipeline = cdf.pipeline()

    # Run the pipeline
    load_info = pipeline.run(us_cities(), table_name="cities", destination="duckdb")

    # Print the load information
    print(load_info)
