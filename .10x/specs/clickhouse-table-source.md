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
Arrow 58 dependency. It MUST stream the official extension's opt-in bounded Arrow query record
batches directly into CDF accounting; row-shaped Serde decoding and a private native/TCP protocol
are forbidden production paths. CDF MAY carry narrow source-compatible patches to those pinned
crates when the official API does not expose pre-allocation response or IPC ceilings; the legacy
unbounded API MUST NOT be reachable from CDF discovery or execution.

## Query, cursor, and consistency semantics

Projection, limit, supported comparison filters, and cursor bounds MUST be generated from typed
plans. Identifiers are driver-validated and values use client bindings; user fragments are never
interpolated. A predicate is `Exact` only when ClickHouse null, collation, decimal, timezone, and
comparison behavior match the Arrow expression. All others remain engine filters.

Physical ClickHouse `UUID` MUST map losslessly to CDF/Arrow `Utf8` as lowercase hyphenated
canonical text. Discovery, projection, exact filters, and ordering MUST generate
`toString(<validated UUID identifier>)` rather than asking ArrowStream to encode `UUID` directly.
Only fields pinned with physical `UUID` metadata receive the validated Binary-to-Utf8 Arrow
adaptation; ordinary ClickHouse `String` remains arbitrary-byte Arrow `Binary`.

Every effective field's physical-type metadata MUST exactly equal the catalog-backed physical
observation before SQL generation. A `DateTime` timezone MUST parse as one nonempty, at-most-128-
byte IANA-style ASCII literal; SQL MUST render only the parsed value, never the raw metadata text.

Snapshot reads and numeric, timestamp, and date cursor incrementality are supported. Cursor order
MUST include a stable user key tie-breaker and use the shared window-close semantics. ClickHouse
native numeric, `Date`/`Date32`, and `DateTime`/`DateTime64` types map to typed cursor values;
strings are not inferred as temporal cursors. Page-token and unsupported mixed positions fail
closed.

Configured `Int8`/`Int16`/`Int32` cursors MUST be projected as Arrow `Int64`, and configured
`UInt8`/`UInt16`/`UInt32` cursors MUST be projected as Arrow `UInt64`. The effective schema MUST
preserve the source physical type and explicit cursor-cast metadata, and all generated projection,
predicate, bound, and ordering expressions MUST apply that same cast. The driver MUST NOT widen a
non-cursor field merely because it has the same physical type.

One resource execution uses one logical query stream. The server may parallelize scanning through
bounded `max_threads`, but the adapter MUST NOT split a mutable table into independently timed
queries and call the result one snapshot. Retry after any emitted uncommitted data restarts the
whole current cursor window under shared source retry authority; it does not splice a second query
into the first stream.

## Types and execution

The source mapping MUST be generated from the official Arrow extension and covered by live
round-trip fixtures for supported booleans, signed/unsigned integers, floats, decimals, strings,
fixed strings/binary, dates, timestamps, arrays, tuples, maps, nullable, low-cardinality, enum,
IPv4/IPv6, and the explicit canonical UUID text mapping. Unsupported
`Dynamic`, variant, aggregate-state, geo, or extension types MUST fail discovery with the exact
field/type unless a separately declared normalization allowance handles them. Decimals never
become floats and timezone metadata is retained.

`UUID`, narrow `Date`, and narrow `DateTime` normalization is admitted only at a top-level field,
where the complete source expression can be cast deterministically. A container that recursively
wraps any of those physical types MUST fail discovery until the pinned Arrow path has an exact
whole-container normalization law. Native `Date32` and `DateTime64` remain supported recursively
because they retain truthful Arrow logical types without that normalization.

Malformed Arrow IPC schemas MUST fail as typed source-data errors before infallible schema
conversion. Unknown enums, invalid integer/decimal widths, invalid union IDs, and wrong nested
child cardinalities MUST NOT panic the process. Schema conversion MUST apply finite field-node,
metadata-entry, nesting-depth, and conservative owned-byte ceilings before allocating the owned
Arrow schema. Malformed record-batch rows, nodes, variadic counts, buffer ranges, union prefixes,
and compressed-output declarations MUST fail before unchecked slicing or unbounded decompression.
The cumulative decoded buffers of a compressed batch MUST remain within the same finite body
authority as an uncompressed batch.

Flat fixed-width projections MUST derive `max_block_size` from aligned validity and value buffers
for every projected field and MUST reject a projection when even one row cannot fit the Arrow body
ceiling. Nested fixed-size projections MUST conservatively take the server-enforced variable-width
path. Because ClickHouse MAY coalesce small internal blocks while serializing ArrowStream, the
decoder's finite record-batch row ceiling MUST be distinct from the performance-oriented
`max_block_size` setting; body bytes remain the hard allocation authority.

The reusable client/pool and Arrow stream are run-owned asynchronous state using injected host
services, cancellation, memory, and egress. Each live cursor MUST retain distinct authority for
its persistent schema/decoder/input-buffer state while per-poll authority MAY reconcile into the
emitted batch. Batches are byte-accounted before admission. The
adapter MUST bound response buffering and MUST NOT create a private executor, queue, pool, or retry
loop. The connection transport buffer, Arrow message/body decoder, and retained discovery model
MUST each have a finite named memory owner before the first lazy query poll. A reusable client/pool
MUST retain its transport lease for at least the complete lifetime of that pooled transport. The
decode envelope MUST account for allocator capacity rather than only logical buffer length and MUST
reserve explicit schema/container headroom. Server-provided type declarations MUST pass finite
iterative text, nesting, and structural-token limits before recursive semantic matching. The
complete retained batch overlap is the configured queue capacity plus one producer-held batch plus
one consumer-held batch. A zero-row record batch MUST reuse the current decode authority rather
than reserve a second full decode lease while the first remains live. Compression and server/client
concurrency follow the measured prepared path.

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
