Status: superseded
Created: 2026-08-02
Updated: 2026-08-03

# ClickHouse destination

Superseded by `.10x/specs/clickhouse-destination.md` after the user required merge support with
native `ReplacingMergeTree` throughput by default and atomic copy-on-write as an opt-in mode.

## Purpose and scope

This specification governed a first-party ClickHouse destination supporting append and a
capability-proven atomic replace. Merge was intentionally unsupported. It was further governed by
`.10x/specs/destination-extension-runtime-contract.md`,
`.10x/specs/destination-receipts-guarantees.md`, and
`.10x/specs/destination-bulk-path-runtime.md`.

## Destination contract

`cdf-dest-clickhouse` was a leaf adapter resolving `clickhouse`/`clickhouses` destination schemes,
secret-backed credentials, and a validated database/table target. The runtime used the official
asynchronous Rust client and `clickhouse-ext-arrow` for Arrow 58. It advertised only append and
replace; merge failed during planning.

The default bulk path was HTTP `ArrowStream` via `insert_arrow()`, with a reused client/pool,
compression, and large byte/row blocks resolved from host memory and measured evidence. Row-shaped
Serde insertion and a private native/TCP implementation were forbidden production fallbacks.

Append was supported only when target inspection proved a MergeTree-family engine and server
deduplication behavior sufficient for deterministic segment insert tokens. Each insert token
derived from destination, target, package hash, and canonical segment identity. Async inserts were
admitted only with `wait_for_async_insert=1`; otherwise the adapter used acknowledged synchronous
inserts. Unsupported dependent materialized views failed preflight.

## Replace and settlement

Replace built and verified a target-compatible staging table/object, then performed one
server-atomic metadata exchange. The exchanged object carried an immutable package settlement
marker even for zero-row packages. `TRUNCATE` followed by insert, mutations, lightweight deletes,
sequential renames, and eventually consistent view changes were not replace.

Append settlement was a recoverable protocol. Every target row carried compact lossless CDF
package/ordinal provenance. Segment inserts were create-or-verify under deterministic deduplication
tokens. Connector-owned load/segment/state mirrors recorded completion after target verification.
The package hash was the logical idempotency token.

## Mapping and capability boundaries

The destination mapping preferred exact native ClickHouse types. Identifier normalization/quoting
was ClickHouse-owned and `_cdf_*` reserved. Existing schema, engine, sorting/partition keys,
projections, materialized views, and codecs were inspected before mutation. CDF did not silently
replace an operator's engine or tuning.

## Execution and performance

Concurrency was injected and bounded. The direct-library roofline followed
`.10x/specs/database-connector-roofline.md`.

## Explicit exclusions

Merge/`ReplacingMergeTree` upsert claims, eventual background deduplication as receipt evidence,
unacknowledged async inserts, generic transactions, arbitrary mutations, private wire protocols,
and silent engine replacement were excluded.
