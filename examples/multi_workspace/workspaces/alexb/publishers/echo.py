"""Echo the model data to stdout"""


def echo(data):
    print(data.payload)


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
