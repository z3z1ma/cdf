Status: active
Created: 2026-08-08
Updated: 2026-08-08

# PostgreSQL native query source

## Purpose and authority

This specification extends `.10x/specs/postgres-source-binary-copy.md` and the existing PostgreSQL
table source with native read queries. It supersedes the arbitrary-SQL exclusion only for the
read-only contract below. Binary COPY, exact type casts, schema descriptors, memory admission,
repeatable execution, errors, and roofline requirements remain authoritative.

## Resource contract

A PostgreSQL resource MUST set exactly one of:

- `table`: the current validated relation identifier; or
- `query`: one PostgreSQL read query producing the source relation.

`query` accepts PostgreSQL `SELECT`, `WITH ... SELECT`, `VALUES`, set operations, joins,
aggregations, windows, lateral references, and source-native read functions. It MUST contain one
statement. CDF rejects DDL, DML, `COPY`, transaction/session control, `CALL`, and known write forms
before contact, then prepares, discovers, and executes it inside a server-enforced read-only
transaction. Authored functions execute under the configured PostgreSQL role; that role remains
the permission authority for functions with external effects.

Resource controls are adapter-native:

| Option | Contract |
| --- | --- |
| `isolation` | `read_committed`, `repeatable_read` (default), or `serializable` |
| `statement_timeout_ms` | optional `1..=3600000`, applied transaction-locally |
| `lock_timeout_ms` | optional `1..=3600000`, applied transaction-locally |
| `output_batch_rows` | optional `1..=100000`, a row ceiling below the existing byte ceiling |
| `search_path` | optional ordered nonempty identifier list, set transaction-locally |

PostgreSQL does not expose a fake server-cursor page option: the production transport remains one
binary `COPY (SELECT ...) TO STDOUT` stream. `output_batch_rows` bounds Arrow publication while the
COPY decoder and byte admission own transport framing.

## Discovery and execution

Discovery prepares/describes the exact query in a read-only transaction without executing payload
rows. The output descriptor is canonical CDF schema authority. Table resources retain catalog
discovery. Query text, local settings, resolved output descriptor, and source generation evidence
participate in compiled identity; diagnostics and explain use a hash plus bounded shape summary,
not query literals.

The native query result is the relation consumed by surrounding CDF SQL. The adapter wraps it as a
derived relation and applies exact CDF projection, predicates, cursor bounds, stable ordering, and
limit outside it. The canonical outer projection casts every selected field to the exact compiled
PostgreSQL output type before binary COPY. Inexact operations remain engine residuals.

Cursor resources require compatible cursor and stable-key fields in the query output. One run uses
one read-only transaction and one COPY stream; retry reopens the complete uncommitted cursor window
and never splices snapshots. Portable preflight re-describes the query output and rejects changed
schema/authority before effects.

## Safety and diagnostics

The query is trusted project code but credentials are never accepted in it. The server transaction
is explicitly read-only even after local classification. Settings use typed transaction-local
commands and cannot weaken read-only, memory, cancellation, timeout, or checkpoint authority.
PostgreSQL parse/prepare/COPY errors retain server provenance without rendering the full query.

## Acceptance scenarios

- Joins, CTEs, aggregates, windows, native functions, and `VALUES` discover and stream through the
  binary COPY path with exact output types.
- DDL/DML/COPY/CALL/multiple statements fail before payload execution; a disguised server write is
  rejected by the read-only transaction.
- Table and query resources support outer CDF projection/filter/order/limit/cursor behavior,
  package/replay/checkpoint laws, plan portability, cancellation, and live progress.
- Query changes, output descriptor changes, or transaction-local option changes invalidate plan
  identity and preflight.
- A live PostgreSQL query cell compares CDF to equivalent official-client binary COPY and retains
  the connector's governed throughput floor.

## Exclusions

PostgreSQL native queries do not authorize writes, stored-procedure calls, multiple statements,
credential literals, ambient session mutation, or CDC logical replication outside its separate
mode.
