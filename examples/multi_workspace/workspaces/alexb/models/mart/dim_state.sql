MODEL (
  name mart.dim_state,
  kind VIEW,
  cron "@daily",
  description "States in the US",
  tags (geo, usa),
  owner "AlexB",
  audits (
    not_null(columns = [us_state]),
    unique_values(columns = [us_state]),
  ),
  grain us_state
);

SELECT
  us_state,
  min(us_latitude) as us_latitude_min,
  max(us_latitude) as us_latitude_max,
  min(us_longitude) as us_longitude_min,
  max(us_longitude) as us_longitude_max,
  count(distinct us_county) as us_county_count,
  count(distinct us_city) as us_city_count,
  count(*) as us_zipcode_count
FROM
  cdf_staging.stg_us_zip_codes_metrics_v1__us_cities
GROUP BY
  1
  
