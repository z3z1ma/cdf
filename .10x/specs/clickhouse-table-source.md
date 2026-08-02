Status: active
Created: 2026-08-02
Updated: 2026-08-02

# ClickHouse table source

## Purpose and scope

This specification governs a first-party finite ClickHouse table source through the official Rust
client's ArrowStream extension. It is further governed by
`.10x/specs/source-extension-runtime-contract.md`,
`.10x/specs/resource-authoring-planning-batches.md`, and
`.10x/decisions/non-file-window-close-checkpoint-semantics.md`.

## Source contract

`cdf-source-clickhouse` MUST be a leaf adapter with driver id and source kind `clickhouse`, and
`clickhouse`/`clickhouses` source schemes. Connection credentials MUST remain secret references;
the compiled plan retains only the redacted endpoint/database authority, validated table target,
and fixed query/type policy. Compilation and portable-plan validation perform no contact.

The initial resource is one table, not arbitrary user SQL. Discovery MUST use bounded ClickHouse
metadata operations and freeze the exact Arrow schema before execution. Source contact obeys the
compiled egress scope and current official security-supported server window.

Execution MUST use the official `clickhouse` client with `clickhouse-ext-arrow` matching CDF's
Arrow 58 dependency. It MUST stream `fetch_arrow()` record batches directly into CDF accounting;
row-shaped Serde decoding and a private native/TCP protocol are forbidden production paths.

## Query, cursor, and consistency semantics

Projection, limit, supported comparison filters, and cursor bounds MUST be generated from typed
plans. Identifiers are driver-validated and values use client bindings; user fragments are never
interpolated. A predicate is `Exact` only when ClickHouse null, collation, decimal, timezone, and
comparison behavior match the Arrow expression. All others remain engine filters.

Snapshot reads and numeric, timestamp, and date cursor incrementality are supported. Cursor order
MUST include a stable user key tie-breaker and use the shared window-close semantics. ClickHouse
native numeric, `Date`/`Date32`, and `DateTime`/`DateTime64` types map to typed cursor values;
strings are not inferred as temporal cursors. Page-token and unsupported mixed positions fail
closed.

One resource execution uses one logical query stream. The server may parallelize scanning through
bounded `max_threads`, but the adapter MUST NOT split a mutable table into independently timed
queries and call the result one snapshot. Retry after any emitted uncommitted data restarts the
whole current cursor window under shared source retry authority; it does not splice a second query
into the first stream.

## Types and execution

The source mapping MUST be generated from the official Arrow extension and covered by live
round-trip fixtures for supported booleans, signed/unsigned integers, floats, decimals, strings,
fixed strings/binary, dates, timestamps, arrays, tuples, maps, nullable, low-cardinality, enum,
IPv4/IPv6, and UUID forms that the selected client version represents exactly. Unsupported
`Dynamic`, variant, aggregate-state, geo, or extension types MUST fail discovery with the exact
field/type unless a separately declared normalization allowance handles them. Decimals never
become floats and timezone metadata is retained.

The reusable client/pool and Arrow stream are run-owned asynchronous state using injected host
services, cancellation, memory, and egress. Batches are byte-accounted before admission. The
adapter MUST bound response buffering and MUST NOT create a private executor, queue, pool, or retry
loop. Compression and server/client concurrency follow the measured prepared path.

## Error behavior

Authentication, DNS/TLS/network, server quota, query, schema mismatch, partial-stream, cancellation,
and unsupported-type failures retain source provenance and redaction. Because ClickHouse can emit
rows before a later query error, the adapter MUST treat the window as incomplete unless the stream
terminates successfully; emitted data cannot authorize checkpoint advancement.

## Scenarios and acceptance criteria

- Discovery and execution agree on the same Arrow schema for every admitted live type fixture.
- Cursor windows with duplicate cursor values remain deterministic through the stable tie-breaker
  and produce the shared numeric/date/timestamp window-close position.
- A server error after one emitted batch produces no committed checkpoint and a retry cannot mix
  query instances.
- Source add/discovery/plan/preview/run/replay/redaction/cancellation/jobs-invariance and connector
  certification pass by leaf/catalog/fixture changes only.
- The source macro cell meets the 0.90 official ArrowStream direct-library roofline.

## Explicit exclusions

Resident CDC, arbitrary SQL, private native protocol work, implicit string-to-temporal cursors,
cross-query snapshot claims, and unsupported ClickHouse dynamic/aggregate types are excluded.
