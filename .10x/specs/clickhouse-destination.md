Status: active
Created: 2026-08-02
Updated: 2026-08-02

# ClickHouse destination

## Purpose and scope

This specification governs a first-party ClickHouse destination supporting append and a
capability-proven atomic replace. Merge is intentionally unsupported. It is further governed by
`.10x/specs/destination-extension-runtime-contract.md`,
`.10x/specs/destination-receipts-guarantees.md`, and
`.10x/specs/destination-bulk-path-runtime.md`.

## Destination contract

`cdf-dest-clickhouse` MUST be a leaf adapter resolving `clickhouse`/`clickhouses` destination
schemes, secret-backed credentials, and a validated database/table target. The runtime uses the
official asynchronous Rust client and `clickhouse-ext-arrow` for Arrow 58. It MUST advertise only
append and replace; a merge request fails during planning with remediation to use a CDF-managed
future merge contract or another destination.

The default bulk path is HTTP `ArrowStream` via `insert_arrow()`, with a reused client/pool,
compression, and large byte/row blocks resolved from host memory and measured evidence. Row-shaped
Serde insertion and a private native/TCP implementation are forbidden production fallbacks.

Append is supported only when target inspection proves a MergeTree-family engine and server
deduplication behavior sufficient for deterministic segment insert tokens. Each insert token MUST
derive from destination, target, package hash, and canonical segment identity. Async inserts MAY
be selected only when the server capability is supported and `wait_for_async_insert=1`; otherwise
the adapter uses acknowledged synchronous inserts. Dependent materialized views MUST either fall
within the server's proven deduplication guarantee or fail preflight.

## Replace and settlement

Replace MUST build and fully verify a target-compatible staging table/object, then perform one
server-atomic metadata exchange that makes the complete new target visible. The exchanged object
MUST carry an immutable package settlement marker even for zero-row packages. Inspection MUST
prove the database/table engine, topology, privileges, metadata-marker behavior, and atomic exchange
before advertising the prepared replace path for that target. `TRUNCATE` followed by insert,
mutations, lightweight deletes, sequential renames, or an eventually consistent view change are
not replace.

Append settlement is a recoverable protocol rather than a fabricated multi-table transaction.
Every target row carries compact lossless CDF package/segment/ordinal provenance. Segment inserts
are create-or-verify under deterministic deduplication tokens. A connector-owned load/segment/state
mirror records completion after target verification. If a crash leaves verified target segments
without the mirror, replay MUST verify those exact segments and finish settlement without rewriting
them. The mirror choreography MUST use typed shared receipt/state models, but it MUST NOT claim the
transactional `cdf-dest-sql` backend contract unless ClickHouse can actually provide it.

The package hash is the logical idempotency token. A receipt is returned only after every segment,
count, schema, target, disposition, and settlement fact is independently queryable. Generic
orchestration alone records the package receipt and advances the checkpoint.

## Mapping and capability boundaries

The destination mapping MUST prefer native ClickHouse booleans, signed/unsigned integers, floats,
decimals within declared precision, strings/fixed strings, dates, timestamps, arrays, tuples,
maps, nullable, low-cardinality, enums, UUID, and IP types when Arrow conversion is exact.
Timezone and decimal scale are explicit plan authority. Unsupported Arrow unions, run-end encoding,
and values outside native domains fail before mutation; there is no silent JSON/string fallback.

Identifier normalization/quoting is ClickHouse-owned and `_cdf_*` is reserved. Existing schema,
engine, sorting/partition keys, projections, materialized views, and codecs are inspected before
mutation. CDF MUST NOT silently replace an operator's engine or tuning.

## Execution and performance

Concurrency is injected and bounded. The adapter MAY overlap encoding and acknowledged independent
segment inserts only within declared useful writers, memory, and server limits; it MUST join every
operation before final settlement. Cancellation stops new inserts and preserves enough evidence
for deterministic verify/redrive. Settings and timing are run evidence, not package identity.

The direct-library roofline follows `.10x/specs/database-connector-roofline.md`.

## Scenarios and acceptance criteria

- Replaying an acknowledged append segment with the same deterministic token does not duplicate it,
  including the supported async-insert/materialized-view matrix.
- A crash after target segment insertion but before mirror settlement is recovered by exact target
  verification and mirror completion; no checkpoint advances early.
- Replace exposes either the complete old target or complete new target and verifies zero-row as
  well as nonempty packages through the exchanged settlement marker.
- Merge and targets lacking required append/replace capabilities fail during planning.
- Destination sheet, planning, health, receipt/replay/crash, redaction, jobs-invariance, live type
  round trips, and connector certification pass.
- The destination macro cell meets the 0.90 official ArrowStream direct-library roofline.

## Explicit exclusions

Merge/`ReplacingMergeTree` upsert claims, eventual background deduplication as receipt evidence,
unacknowledged async inserts, generic transactions, arbitrary mutations, private wire protocols,
and silent engine replacement are excluded.
