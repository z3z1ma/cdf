"""A publisher that pushes data to httpbin.org"""

import requests

import cdf

w = cdf.get_workspace(__file__).unwrap()
context = w.to_transform_context("local")

df = context.fetchdf("SELECT * FROM mart.zips")

zip_ = df.iloc[0, 0]

r = requests.post(
    "https://httpbin.org/post",
    data={"zip": zip_},
)
r.raise_for_status()
print(r.json())
