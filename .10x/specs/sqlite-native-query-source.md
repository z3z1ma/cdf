Status: active
Created: 2026-08-08
Updated: 2026-08-09

# SQLite native query source

## Purpose and authority

This specification extends `.10x/specs/sqlite-table-source.md` with native read queries and
inheritable operational controls. It supersedes that specification's arbitrary-query
exclusion. Read-only connection/snapshot, dynamic-type reconciliation, cursor, portability,
memory, cancellation, error, and roofline requirements remain authoritative.

## Resource contract

A SQLite resource MUST set exactly one of `table` or `query`. A query may use SQLite `SELECT`,
`WITH ... SELECT`, `VALUES`, joins, aggregates, windows, JSON functions, virtual tables, and other
read expressions supported by the linked SQLite. It MUST prepare as exactly one statement and
`sqlite3_stmt_readonly` MUST prove it read-only. The connection is also opened read-only with
`query_only` defense in depth.

Operational controls MAY be declared on the configured source and MAY be overridden by a resource.
Resolution order is the built-in default, then source default, then explicit resource override:

| Option | Contract |
| --- | --- |
| `output_batch_rows` | `1..=100000`, default `32768` (the measured passing SQLite roofline default) |
| `busy_timeout_ms` | optional `1..=3600000`, connection-local |
| `cache_kib` | optional `64..=1048576`, connection-local and non-persistent |
| `mmap_bytes` | optional `0..=1073741824`, connection-local and non-persistent |

Persistent pragma changes, extension loading, attach/detach, and multi-statement input remain
forbidden. Changing a control changes compiled identity.

## Discovery and execution

Table discovery retains declared/catalog types and never samples rows. Query discovery first uses
prepared output metadata. When an SQLite expression has no declared result type, discovery MAY
observe rows from the exact query within the caller-owned discovery request budget because runtime
storage classes can differ. Row/byte limits are discovery-command policy, MUST NOT be authored as
source or resource options, and cannot claim global uniformity. An explicitly governed schema
remains execution authority. Execution holds one read transaction from first query step through
stream EOF.

The query output becomes the relation consumed by CDF SQL. Exact projection, predicates, cursor
bounds, stable ordering, and limit are wrapped outside the authored query using bound values and
validated identifiers; inexact predicates remain engine residuals. Cursor/key fields must survive
the query output and retain explicit temporal encoding when temporal.

## Acceptance scenarios

- A join/aggregate/JSON query discovers bounded dynamic output and executes from one read snapshot.
- A write pragma, DDL/DML, attach, extension load, or second statement fails before stepping.
- Table and query paths support CDF operations, portable relative paths, package/replay/checkpoint,
  cancellation, progress, and dynamic drift policy.
- Source-default and resource-override batch/cache controls are bounded, identity-bearing,
  non-persistent, and do not
  alter logical package bytes when output is identical.
- Live query and table cells retain the direct-`rusqlite` roofline requirement.

## Exclusions

Native queries do not authorize writes, persistent pragma mutation, extension installation,
network filesystems, resident CDC, or multi-connection snapshot claims.
