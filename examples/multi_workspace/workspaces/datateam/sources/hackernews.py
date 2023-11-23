import time
import typing as t
from datetime import datetime

import dlt
from dlt.sources.helpers import requests

URL = "https://hn.algolia.com/api/v1/search_by_date"


@dlt.source(name="hackernews")
def hn_search(
    keywords: t.List[str] = dlt.config.value,
    start_date: datetime = dlt.config.value,
    end_date: datetime = datetime.today(),
    text: str = "any",
    daily_load: bool = False,
):
    """Source method for the Algolia Hacker News Search API: https://hn.algolia.com/api

    Args:
        keywords: list of keywords for which the data needs to be loaded
        start_date: start date in datetime or "yyyy-mm-dd" format
        end_date: end date in datetime or "yyyy-mm-dd" format
        text: possible values: "story","comment". For any other value, everything is loaded.
        daily_load: loads data in daily intervals when set to True (default: weekly)
    """

    # Read start date as string or datetime and convert it to UNIX timestamp
    if isinstance(start_date, str):
        start_timestamp = int(
            time.mktime(datetime.strptime(start_date, "%Y-%m-%d").timetuple())
        )
    else:
        start_timestamp = int(time.mktime(start_date.timetuple()))  # type: ignore

    # Read end date as string or datetime and convert it to UNIX timestamp
    if isinstance(end_date, str):
        end_timestamp = int(
            time.mktime(datetime.strptime(end_date, "%Y-%m-%d").timetuple())
        )
    else:
        end_timestamp = int(time.mktime(end_date.timetuple()))

    today = int(time.mktime(datetime.today().timetuple()))

    # Don't load the data for dates after the current date
    end_timestamp = min(today, end_timestamp)

    # Ensure that the input start date is smaller than the input end date
    if start_timestamp > end_timestamp:
        raise ValueError(f"{start_date=} is larger than {end_date=}")

    # Specify text = "comment" or text="story" when calling the function
    # to load only comments or stories
    if text in ["comment", "story"]:
        tags = text
    # Pass any other value to load everything (default behaviour)
    else:
        tags = "(story,comment)"

    return keyword_hits(keywords, start_timestamp, end_timestamp, tags, daily_load)


@dlt.resource(name="keyword_hits", write_disposition="append")
def keyword_hits(
    keywords,
    start_timestamp,
    end_timestamp,
    tags,
    daily_load=False,
):
    """This methods makes a call to the Algolia Hacker News and returns all the hits corresponding the the input keywords

    Since the API response is limited to 1000 hits,
    a separate call is made for each keyword for each week between the start and end dates

    If daily_load=True, then a single call is made for each keyword for the previous day

    Args:
        keywords: list of keywords for which the data needs to be loaded
        start_timestamp: UNIX timestamp for the start date
        end_timestamp: UNIX timestamp for the end date
        tags: parameter for the API call to specify "story", "comment" or "(story,comment)"
        daily_load: loads data in daily intervals when set to True (default: weekly)
    """

    def _generate_hits(keyword, batch_start_date, batch_end_date, tags):
        """This function makes the API call and returns all the hits for the input parameters"""
        params = {
            "query": f'"{keyword}"',
            "tags": f"{tags}",
            "numericFilters": f"""created_at_i>={batch_start_date},created_at_i<{batch_end_date}""",
            "hitsPerPage": 1000,
        }
        response = requests.get(URL, params=params)
        response.raise_for_status()

        return response.json()["hits"]

    time_delta = (
        86400 if daily_load else 604800
    )  # The length of a day/week in UNIX timestamp

    # Iterate across all keywords
    for keyword in keywords:
        batch_start_date = start_timestamp
        batch_end_date = batch_start_date + time_delta

        # Iterate across each week between the start and end dates
        while batch_end_date < end_timestamp + time_delta:
            batch_end_date = min(
                batch_end_date, end_timestamp
            )  # Prevent loading data ahead of the end date
            # The response json
            data = _generate_hits(keyword, batch_start_date, batch_end_date, tags)

            for hits in data:
                yield {
                    key: value
                    for (key, value) in hits.items()
                    if not key.startswith(
                        "_"
                    )  # Filtering down to relevant fields from the response json
                }

            batch_start_date = batch_end_date
            batch_end_date += time_delta


# This is the only addition required to an existing dlt source file to get the benefits of cdf
__CDF_SOURCE__ = {
    "hackernews": {
        # factory must be resolvable via cdf config, kwargs should be set to dlt.config.value or populated with defaults / via closure
        "factory": hn_search,
        "version": 1,
        "owners": ("qa-team"),
        "description": "Extracts hackernews data from an API.",
        "tags": ("live", "simple", "test"),
        "metrics": {
            "keyword_hits": {
                "count": lambda _, metric=0: metric + 1,
            }
        },
    }
}

# Also worth metioning, above is exactly the same as:
from cdf import export_sources, source_spec

export_sources(
    hackernews_2=source_spec(
        factory=hn_search,
        version=2,
        owners=("qa-team"),
        description="Extracts hackernews data from an API.",
        tags=("live", "simple", "test"),
        metrics={
            "keyword_hits": {
                "count": lambda _, metric=0: metric + 1,
            }
        },
    )
)

# the difference being that the latter gives type hints and is more readable
# while the former requires no imports of cdf and is thus valid independent of the cdf package
