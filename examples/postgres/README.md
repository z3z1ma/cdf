# Postgres source example

This project reads a local Postgres table and writes it to DuckDB. Create the fixture with your local Postgres client:

```sql
CREATE TABLE public.cdf_example_orders (
  id BIGINT NOT NULL,
  name TEXT,
  updated_at BIGINT NOT NULL
);
INSERT INTO public.cdf_example_orders VALUES
  (1, 'Ada', 10),
  (2, 'Grace', 20);
```

Store the DSN outside TOML, run the example, and remove the temporary secret file when finished:

```bash
printf '%s\n' "$CDF_EXAMPLE_POSTGRES_DSN" > postgres-dsn
chmod 600 postgres-dsn
cdf validate --deep
cdf plan warehouse.orders
cdf run warehouse.orders
cdf sql 'select * from package_files'
rm postgres-dsn
```

`postgres-dsn` is ignored by the repository. The resource contains only its `secret://file/postgres-dsn` reference.
