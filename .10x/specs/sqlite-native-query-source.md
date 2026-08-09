Status: active
Created: 2026-08-08
Updated: 2026-08-08

# SQLite native query source

## Purpose and authority

This specification extends `.10x/specs/sqlite-table-source.md` with native read queries and
resource-scoped operational controls. It supersedes that specification's arbitrary-query
exclusion. Read-only connection/snapshot, dynamic-type reconciliation, cursor, portability,
memory, cancellation, error, and roofline requirements remain authoritative.

## Resource contract

A SQLite resource MUST set exactly one of `table` or `query`. A query may use SQLite `SELECT`,
`WITH ... SELECT`, `VALUES`, joins, aggregates, windows, JSON functions, virtual tables, and other
read expressions supported by the linked SQLite. It MUST prepare as exactly one statement and
`sqlite3_stmt_readonly` MUST prove it read-only. The connection is also opened read-only with
`query_only` defense in depth.

Resource controls are:

| Option | Contract |
| --- | --- |
| `discovery_records` | `1..=100000`, default `1000`, for dynamic expression types |
| `discovery_bytes` | `1024..=67108864`, default `16777216` |
| `output_batch_rows` | `1..=100000`, default `65536` |
| `busy_timeout_ms` | optional `1..=3600000`, connection-local |
| `cache_kib` | optional `64..=1048576`, connection-local and non-persistent |
| `mmap_bytes` | optional `0..=1073741824`, connection-local and non-persistent |

Persistent pragma changes, extension loading, attach/detach, and multi-statement input remain
forbidden. Changing a control changes compiled identity.

## Discovery and execution

Table discovery retains declared/catalog types. Query discovery combines prepared output metadata
with bounded rows from the exact query because SQLite expression columns may have no declared type
and runtime storage classes can differ. The sample records its row/byte limits and cannot claim
global uniformity. Execution holds one read transaction from first query step through stream EOF.

The query output becomes the relation consumed by CDF SQL. Exact projection, predicates, cursor
bounds, stable ordering, and limit are wrapped outside the authored query using bound values and
validated identifiers; inexact predicates remain engine residuals. Cursor/key fields must survive
the query output and retain explicit temporal encoding when temporal.

## Acceptance scenarios

- A join/aggregate/JSON query discovers bounded dynamic output and executes from one read snapshot.
- A write pragma, DDL/DML, attach, extension load, or second statement fails before stepping.
- Table and query paths support CDF operations, portable relative paths, package/replay/checkpoint,
  cancellation, progress, and dynamic drift policy.
- Resource batch/sample/cache controls are bounded, identity-bearing, non-persistent, and do not
  alter logical package bytes when output is identical.
- Live query and table cells retain the direct-`rusqlite` roofline requirement.

## Exclusions

Native queries do not authorize writes, persistent pragma mutation, extension installation,
network filesystems, resident CDC, or multi-connection snapshot claims.
