/* This is a simple model that selects distinct zip codes from the cities table */
MODEL (
    name mart.zips
);

SELECT DISTINCT
    zip_code
FROM us_cities_v0_1.cities
