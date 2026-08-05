RESOURCE
DISPOSITION APPEND
CURSOR updated_at
TRUST GOVERNED
EXECUTION BOUNDED
AS
SELECT *
FROM upstream(
  source => 'api',
  path => '/events',
  records => '$.items',
  cursor_param => 'since',
  cursor_filter_fidelity => 'exact'
);
