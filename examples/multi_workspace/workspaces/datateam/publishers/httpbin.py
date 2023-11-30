import dlt
import requests


def post(data, value: str = dlt.config.value) -> None:
    print(data.payload)
    r = requests.post("https://httpbin.org/post", data={"key": value})
    r.raise_for_status()
    print(r.json())


__CDF_PUBLISHERS__ = [
    {
        "publisher_name": "httpbin",
        "runner": post,
        "from_model": "hackernews_v1.keyword_hits",
        "mapping": {"author": "Author"},
        "version": 1,
        "owners": ("qa-team",),
        "description": "A publisher that pushes data to httpbin.org",
        "tags": ("httpbin", "test"),
        "cron": None,
        "enabled": True,
    }
]
