RESOURCE
DISPOSITION APPEND
CURSOR updated_at
TRUST GOVERNED
EXECUTION BOUNDED
AS
SELECT *
FROM upstream(source => 'warehouse', table => 'public.cdf_example_orders');
