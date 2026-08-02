Status: active
Created: 2026-08-02
Updated: 2026-08-02

# SQLite, ClickHouse, and MongoDB connector roofline

## Purpose and scope

This specification governs performance selection and closure evidence for the first-party SQLite,
ClickHouse, and MongoDB source and destination adapters. It refines
`.10x/specs/performance-lab-and-envelope.md` and `.10x/specs/destination-bulk-path-runtime.md`
without weakening correctness, memory, receipt, or checkpoint requirements.

The user ratified the 90% direct-library roofline option on 2026-08-02.

## Measurement contract

Each direction MUST have a release-mode local macro cell and a same-process or isolated-child
direct baseline using the exact native library and protocol selected by the adapter. Setup,
fixture creation, schema creation, container startup, and dependency download MUST remain outside
the timed region. Source timing begins immediately before the first query/read operation and ends
after every Arrow batch is consumed. Destination timing begins immediately before the first
package segment is offered and ends only after commit-bound or independent receipt verification.

The report MUST include raw samples, median-of-N, dispersion, rows, useful Arrow bytes, physical
wire or file bytes where observable, wall time, CPU time where available, peak RSS, batch sizes,
in-flight bounds, connection/writer count, compression, database/server/client versions, and the
existing performance-lab comparability key. The direct baseline MUST perform the same projection,
type conversion, target mutation, durability, and verification work where the native library can
express it. Any semantic work present on only one side MUST be named as bias.

For each comparable cell:

```text
roofline_ratio = cdf_median_useful_throughput / direct_library_median_useful_throughput
```

The connector MUST reach `roofline_ratio >= 0.90`. A cell with high variance, a changed host,
missing counters, incompatible semantics, or an unavailable dependency is inconclusive rather
than passing. No README or product claim may exceed the recorded evidence.

## Connector-specific saturation

- SQLite MUST compare against direct `rusqlite` prepared statements and explicit transactions on
  the same file, filesystem, journal mode, durability mode, and schema. The adapter MUST keep one
  writer fed and MUST NOT create parallel writers to manufacture CPU utilization.
- ClickHouse MUST compare against the official Rust client plus `clickhouse-ext-arrow` ArrowStream
  path on the same server and table. The sweep MUST cover byte/row block targets, compression,
  connection reuse, server `max_threads`, and bounded client concurrency. The selected default is
  the fastest exact setting within memory and server limits.
- MongoDB MUST compare against the official asynchronous Rust driver on MongoDB 8.0 or later using
  the same raw-BSON cursor or batched write models, write concern, transaction boundary, indexes,
  pool, and document mapping. The sweep MUST cover cursor/write batch bytes, pool size, and bounded
  in-flight operations.

All tuning uses injected CDF execution, memory, cancellation, and pressure authorities. Adapters
MUST NOT construct private runtimes, unbounded queues, hidden semaphores, or retry loops. Adaptive
settings are run evidence, not package identity.

## Local and remote evidence

Deterministic local SQLite files and digest-pinned local ClickHouse/MongoDB containers are the
closure gate. ClickHouse versions MUST be within the official security-supported window;
MongoDB's minimum supported server is 8.0. Dependency and image versions MUST be locked in the
report.

ClickHouse Cloud and MongoDB Atlas cells SHOULD be recorded when credentials are supplied. They
MUST remain visible as unavailable otherwise and do not block initial local closure. Running them
requires separate explicit authorization for credentials, external writes, and cost; this spec
does not grant it.

## Scenarios

Given a fixed local fixture, when the adapter and direct runner execute comparable release-mode
samples, then the report computes the ratio from raw medians and fails closure below 0.90.

Given a slower path that accepts a schema the fastest path cannot represent exactly, when it is
retained, then its distinct capability and measured limit are declared; it is never a silent
fallback.

Given no cloud credentials, when closure runs, then local gates execute and remote cells remain
explicitly unavailable without attempting external contact.

## Acceptance criteria

- Six comparable local cells meet the 0.90 roofline or remain open with measured evidence.
- Memory and in-flight work remain bounded under slow-consumer and cancellation tests.
- Selected batch, compression, connection, and writer settings are declared in capability data and
  observable in run evidence.
- Certification and correctness suites pass with the same production path measured by the macro
  cells.

## Explicit exclusions

This specification does not authorize benchmark-only production bypasses, reduced durability,
unverified writes, unbounded concurrency, cloud spending, or marketing claims from laptop-only
evidence.
