"""Echo the model data to stdout"""
import pandas as pd

import cdf


@cdf.with_config(sections=("whatever",))
def echo(data, key=cdf.inject_config) -> None:
    """The echo publisher

    Args:
        data (cdf.Payload): The data to publish. First pos arg is supplied by cdf based on the from_model
    """
    df: pd.DataFrame = data.payload
    print(df)
    print(key)


__CDF_PUBLISHERS__ = [
    {
        "name": echo.__name__,
        "runner": echo,
        "from_model": "mart.dim_state",
        "mapping": {
            "us_state": "US State",
            "us_latitude_min": "Latitude Min",
            "us_latitude_max": "Latitude Max",
            "us_longitude_min": "Longitude Min",
            "us_longitude_max": "Longitude Max",
            "us_city_count": "City Count",
            "us_zipcode_count": "Zipcode Count",
        },
        "version": 1,
        "owners": ["Alex B"],
        "description": __doc__,
        "tags": ["echo"],
    }
]
