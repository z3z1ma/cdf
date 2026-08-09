# PostgreSQL source

CDF reads PostgreSQL through one binary `COPY (SELECT ...) TO STDOUT` stream. A resource chooses
exactly one native input: a table or a read query.

Reusable transaction and output policy belongs on the configured source:

```toml
[sources.warehouse]
type = "postgres"
connection = "secret://env/WAREHOUSE_URL"
isolation = "repeatable_read"
statement_timeout_ms = 300000
lock_timeout_ms = 5000
output_batch_rows = 65536
search_path = ["analytics", "public"]
```

```sql
-- Table input
FROM upstream(source => 'warehouse', table => 'public.orders')

-- Native query input
FROM upstream(
  source => 'warehouse',
  query => 'WITH recent AS (
              SELECT id, account_id, amount, updated_at
              FROM ledger
              WHERE updated_at >= DATE ''2026-01-01''
            )
            SELECT id, account_id, amount, updated_at,
                   sum(amount) OVER (PARTITION BY account_id ORDER BY updated_at) AS running_amount
            FROM recent',
  isolation => 'repeatable_read',
  statement_timeout_ms => 300000,
  lock_timeout_ms => 5000,
  output_batch_rows => 65536,
  search_path => ARRAY['analytics', 'public']
)
```

Native queries support PostgreSQL `SELECT`, `WITH`, `VALUES`, joins, aggregates, windows, set
operations, lateral references, and read functions. CDF rejects multiple statements, DDL, DML,
`COPY`, `CALL`, row locks, `SELECT INTO`, parameters, and transaction or session commands. It then
prepares and executes the query in a server-enforced read-only transaction. The configured
PostgreSQL role remains the authority for function permissions and external effects.

Each control may be a source default or a resource override. Resolution is built-in default, then
source default, then explicit resource override:

- `isolation`: `read_committed`, `repeatable_read` (default), or `serializable`.
- `statement_timeout_ms` and `lock_timeout_ms`: optional values from 1 through 3,600,000,
  transaction-local.
- `output_batch_rows`: optional Arrow publication ceiling from 1 through 100,000. Its default is
  the measured 65,536-row production setting. The native binary COPY transport and byte admission
  remain unchanged.
- `search_path`: an ordered, nonempty list of PostgreSQL identifiers, transaction-local.

Discovery prepares the query without reading payload rows. The exact output descriptor, query,
and controls bind compiled and portable-plan identity. Portable-plan preflight describes the query
again and fails before package or destination effects if its output authority changed. Human
reports show a bounded hash/shape summary; the exact query remains in compiled project artifacts,
so those artifacts should receive the same access controls as project source.

PostgreSQL's prepared output descriptor does not expose `NUMERIC` typmods. Native-query `NUMERIC`
columns therefore use CDF's exact PostgreSQL numeric-text semantic representation. Cast in the
authored query when a different supported output domain is required.
