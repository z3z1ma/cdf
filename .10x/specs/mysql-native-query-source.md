Status: active
Created: 2026-08-08
Updated: 2026-08-09

# MySQL finite native query source

## Purpose and scope

This specification governs the finite-read mode of the new `cdf-source-mysql` crate. The same
crate and source kind later own MySQL ROW/FULL/GTID CDC under the CDC specifications; there is no
parallel `mysql_cdc` source kind. This adapter follows
`.10x/decisions/connector-native-capability-before-commons.md` and does not wait for or introduce a
shared SQL-source grammar.

MySQL 8.4 LTS is the initial server floor. Credentials remain secret references. Compilation is
contact-free and compiled artifacts retain only redacted endpoint/database authority, canonical
resource intent, schema/generation evidence, and bounded transport policy.

## Resource contract

A finite resource MUST set exactly one of `table` or `query`. A query may use one MySQL `SELECT`,
`WITH ... SELECT`, `VALUES`, joins, unions, aggregation, windows, JSON functions, and native read
expressions permitted by the configured role. It MUST be one statement. CDF rejects DDL, DML,
`INTO OUTFILE`/`DUMPFILE`, `LOAD`, `CALL`, transaction/session control, and multiple statements;
execution also uses a read-only transaction and a source account without write authority.

These controls MAY be configured as source defaults and MAY be overridden by a resource.
Resolution order is built-in default, then source default, then explicit resource override:

| Option | Contract |
| --- | --- |
| `isolation` | `read_committed`, `repeatable_read` (default), or `serializable` |
| `fetch_rows` | `1..=100000`, default `8192`; server cursor fetch window |
| `output_batch_rows` | `1..=100000`, default `65536` |
| `max_execution_time_ms` | optional `1..=3600000` |
| `lock_wait_timeout_ms` | optional `1..=3600000`, transaction-local |
| `use_invisible_indexes` | boolean, default `false`, transaction-local |

Source configuration additionally owns TLS, authentication, pool limits, shared in-flight work,
and default database. Resolved controls are canonical compiled inputs and receive no ambient
environment override.

## Discovery, types, and execution

Table discovery uses MySQL catalog metadata. Query discovery prepares/describes the exact statement
without executing rows. The adapter freezes ordered field names, native type/flags/collation,
nullability evidence, key/cursor candidates where provable, and a secret-safe generation hash.

The native query output is the relation consumed by surrounding CDF SQL. Exact outer projection,
predicates, cursor bounds, stable ordering, and limit are rendered with MySQL bindings; inexact
operations remain engine residuals. Query cursor/key fields must survive output.

Execution uses a production-maintained asynchronous Rust MySQL client selected and pinned by the
implementation ticket, and its binary protocol with a server-side cursor or equivalent genuinely
streaming prepared-result path. It MUST NOT issue one
network request per row, materialize the full result, stringify native values for convenience, or
create a private runtime/retry loop. Fetch and Arrow output windows are independently bounded and
backpressure propagates through the ordinary source/package pipeline.

One run holds one read-only consistent-snapshot transaction through EOF. Retry restarts the entire
uncommitted cursor window. A destination receipt gates checkpoint advancement. Finite execution
does not publish a binlog position as CDC authority; snapshot-to-CDC handoff is separately
governed.

Native booleans/integers (signed and unsigned), floats, exact DECIMAL, binary/text with collation,
date/time/timestamp, JSON, enum/set, bit, UUID conventions, spatial values, zero dates, and invalid
temporal domains require an explicit live type sheet. Decimal never becomes float, binary never
silently becomes UTF-8, timezone meaning is explicit, and unsupported values fail or use a declared
lossless semantic text/binary mapping.

## Acceptance scenarios

- Table and complex query resources discover and stream exact binary values from one read-only
  consistent snapshot.
- Write/file/session/multiple statements fail before payload execution and the server transaction
  independently refuses writes.
- Fetch/output controls are independently bounded; equivalent settings preserve logical package
  identity and checkpoint results.
- Outer CDF SQL, cursor windows, add/discovery/plan/preview/run/replay, portable preflight,
  cancellation, progress, redaction, packages, receipts, and checkpoints pass live MySQL 8.4
  certification.
- A same-host direct-client roofline using the identical pinned client proves at least the governed
  0.90 ratio for representative
  narrow, wide, decimal, text/binary, JSON, and cursor workloads.

## Exclusions

This finite contract does not define binlog decoding, snapshot-to-log handoff, destination writes,
multi-statement scripts, stored-procedure calls, credential literals, or a generic SQL dialect.
