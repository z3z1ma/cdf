import dlt
import requests

from cdf import export_pipelines, pipeline_spec


@dlt.resource(write_disposition="replace")
def us_cities():
    yield requests.get(
        "https://raw.githubusercontent.com/millbj92/US-Zip-Codes-JSON/master/USCities.json"
    ).json()


@dlt.source
def us_zip_codes():
    return [us_cities]


export_pipelines(
    pipeline_spec(us_zip_codes),
    pipeline_spec(
        us_zip_codes,
        "us_zip_codes_metrics",
        metrics={
            us_cities.__name__: {
                "count": lambda item, metric: metric + 1,
                "longest_name": lambda item, metric: max(metric, len(item["city"])),
                "min_latitude": lambda item, metric: min(
                    metric, item["latitude"] or metric
                ),
                "max_latitude": lambda item, metric: max(
                    metric, item["latitude"] or metric
                ),
                "min_longitude": lambda item, metric: min(
                    metric, item["longitude"] or metric
                ),
                "max_longitude": lambda item, metric: max(
                    metric, item["longitude"] or metric
                ),
            }
        },
    ),
)
