"""A publisher that pushes data to httpbin.org"""
import dlt
import pandas as pd
import requests


def post(data, test_str: str = dlt.config.value) -> None:
    """Post the data to httpbin.org

    Args:
        data (dlt.Payload): The data to publish. First pos arg is supplied by cdf based on the from_model
        test_str (str, optional): A test string to post. Injected from cdf_config.toml
    """
    df: pd.DataFrame = data.payload
    r = requests.post(
        "https://httpbin.org/post",
        data={"hn_person": df["Author"].iloc[0], "test_str": test_str},
    )
    r.raise_for_status()
    print(r.json())


__CDF_PUBLISHERS__ = [
    {
        "name": "httpbin",
        "runner": post,
        "from_model": "hackernews_v1.keyword_hits",
        "mapping": {"author": "Author"},
        "version": 1,
        "owners": ("qa-team",),
        "description": __doc__,
        "tags": ("httpbin", "test"),
        "cron": None,
        "enabled": True,
    }
]
