Status: active
Created: 2026-08-08
Updated: 2026-08-08

# ClickHouse native query source

## Purpose and authority

This specification extends `.10x/specs/clickhouse-table-source.md` with native read queries and
resource-scoped ClickHouse settings. It supersedes that specification's arbitrary-SQL exclusion.
Official ArrowStream transport, type fidelity, response/IPC limits, query-instance consistency,
errors, cancellation, and roofline requirements remain authoritative.

## Resource contract

A ClickHouse resource MUST set exactly one of `table` or `query`. A query may use one ClickHouse
`SELECT`/`WITH ... SELECT`, joins, unions, aggregation, windows, dictionaries, table functions, and
server-native read expressions permitted by the configured role. It MUST be one statement. CDF
rejects DDL, DML, `INSERT ... SELECT`, `INTO OUTFILE`, session control, and known mutation forms,
and every request applies ClickHouse `readonly = 2` as server-enforced defense in depth.

Resource controls are:

| Option | Contract |
| --- | --- |
| `max_block_rows` | optional `1..=1000000`, still capped by Arrow body/row authority |
| `max_threads` | optional `1..=256`, default chosen from admitted host concurrency |
| `max_execution_time_ms` | optional `1..=3600000` |
| `max_rows_to_read` | optional positive integer |
| `max_bytes_to_read` | optional positive integer |
| `max_memory_usage` | optional positive bytes not exceeding admitted process authority |
| `settings` | optional adapter-validated map of ClickHouse scalar settings |

`settings` permits server-native read/performance controls but MUST reject unknown non-scalar
values and any key that can write, weaken `readonly`, bypass CDF response/memory bounds, enable
credential-bearing external access, or mutate ambient/session state. Explicit typed options win;
duplicates fail rather than silently override.

## Discovery and execution

Discovery describes the exact native query and freezes its Arrow output before payload execution.
The query and settings hash, output schema/physical metadata, and server object-generation evidence
participate in identity. Portable preflight re-describes output and validates enumerated table/
dictionary dependencies when available; an opaque dependency makes the plan non-portable rather
than under-attested.

The native query output is wrapped as the relation consumed by CDF SQL. Exact projection,
predicates, cursor bounds, ordering, and limit apply outside the query and are pushed through the
adapter only when ClickHouse semantics match. Inexact operations remain residuals. Cursor and
stable-key fields must survive query output. One execution remains one ArrowStream query instance;
retry restarts the complete uncommitted window.

## Acceptance scenarios

- Joins, dictionaries, table functions, aggregates, windows, and unions discover and stream their
  exact Arrow output through the bounded official extension.
- Writes, output files, session mutations, multiple statements, and attempts to override read-only
  or CDF allocation limits fail before payload execution.
- Table/query resources support CDF operations, cursor laws, plan portability, package/replay/
  checkpoint, cancellation, progress, and redacted diagnostics.
- Native settings are identity-bearing and bounded; harmless performance changes do not alter
  package bytes for identical logical output.
- Live query and table cells retain the official ArrowStream direct-library roofline.

## Exclusions

Native queries do not authorize writes, ambient settings, private protocols, credential-bearing
external table functions without typed secret/egress authority, or resident CDC.
