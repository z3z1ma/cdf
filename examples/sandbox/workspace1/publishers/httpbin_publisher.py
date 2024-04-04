"""A publisher that pushes data to httpbin.org"""

import requests

import cdf

project = cdf.find_nearest(__file__).unwrap()
w = project.get_workspace("workspace1").unwrap()
context = w.to_transform_context("local")

df = context.fetchdf("SELECT * FROM mart.zips")

zip_ = df.iloc[0, 0]

r = requests.post(
    "https://httpbin.org/post",
    data={"zip": zip_},
)
r.raise_for_status()
print(r.json())
