Status: active
Created: 2026-08-03
Updated: 2026-08-03

# ClickHouse destination

## Purpose and scope

This specification governs the first-party ClickHouse destination supporting append, atomic
replace, and keyed merge. Merge defaults to ClickHouse-native `ReplacingMergeTree` throughput; an
environment policy opts into atomic copy-on-write publication when immediate physical uniqueness
is worth rewriting the target. It is further governed by
`.10x/specs/destination-extension-runtime-contract.md`,
`.10x/specs/destination-receipts-guarantees.md`,
`.10x/specs/destination-bulk-path-runtime.md`, and
`.10x/specs/spillable-package-dedup.md`.

## Configuration contract

The destination URI remains a credential-safe `clickhouse://host/database` or
`clickhouses://host/database` reference with no query parameters. Merge mode is an environment
destination policy, not connection identity:

```toml
[environments.prod.destination_policy.clickhouse]
merge_mode = "atomic_copy_on_write"
```

When omitted, `merge_mode` MUST resolve to `replacing_merge_tree`. The only admitted values are
`replacing_merge_tree` and `atomic_copy_on_write`. The resolved value MUST participate in the
prepared plan id and receipt transaction metadata. Changing it between a package's planning and
replay MUST fail closed rather than reinterpret an existing settlement.

## Shared destination and data-plane contract

`cdf-dest-clickhouse` MUST resolve the `clickhouse`/`clickhouses` schemes, secret-backed
credentials, and validated database/table target. It uses the official asynchronous Rust client
and `clickhouse-ext-arrow` ArrowStream path with a reused client, LZ4 compression, bounded input
batches, and injected memory authority. Row-shaped Serde payload insertion and a private native/TCP
implementation are forbidden production fallbacks.

Append, replace, and merge MUST carry compact lossless package-hash and canonical row-ordinal
provenance. Every payload insert uses synchronous acknowledgement (`async_insert=0`,
`wait_for_async_insert=1`) and a deterministic token derived from destination, target, package,
mode, and canonical segment identity. Connector-owned typed load/segment/state mirrors are written
with the receipt marker last. Their exact schemas are capability-validated before use.

Existing schema, engine, sorting/partition keys, projections, dependent materialized views, and
codecs MUST be inspected before mutation. The connector MUST NOT silently create a user target,
replace its engine, or weaken its tuning. A dependent materialized view fails merge preflight
because replacement semantics cannot truthfully retract its prior effects.

## Default native merge

`replacing_merge_tree` MUST be the default keyed merge mode. It writes canonical package segments
directly through ArrowStream and relies on ClickHouse's native replacement machinery. It MUST NOT
run a target rewrite, mutation, `OPTIMIZE FINAL`, or client-side row loop.

The target MUST already use unversioned `ReplacingMergeTree` or
`ReplicatedReplacingMergeTree`. Its simple sorting-key identifier vector MUST equal the normalized
CDF merge-key vector exactly and in order. The table MUST be unpartitioned, or its simple
partition-key identifier vector MUST be a subset of the merge keys, proving equal merge keys cannot
land in different partitions. Expressions that prevent this proof fail preflight. Merge keys MUST
exist in the mapped schema and MUST be non-nullable. Package-level deterministic dedup remains the
semantic authority; destination preflight additionally rejects a package/schema that cannot prove
one canonical row per merge key.

The commit MUST synchronously acknowledge every inserted segment, then verify the package's
logical winning rows with `FINAL` before settlement. A receipt may settle only after the logical
`FINAL` snapshot contains exactly the package's expected winning keys and values. Historical merge
receipt verification uses immutable exact settlement mirrors; it MUST NOT claim superseded package
rows remain current after a later successful merge. Crash replay before settlement uses package
provenance plus deterministic insert tokens to verify/redrive the same logical write.

Ordinary non-`FINAL` readers MAY observe multiple physical versions until ClickHouse background
merges compact them. This latency/visibility tradeoff MUST be stated in planning, documentation,
and receipt metadata. `EffectivelyOncePerKey` refers to the logical `FINAL` view; it is not a claim
of immediate physical uniqueness. Exact inserted-versus-updated counts MAY be absent in this mode
when producing them would require a target-wide join; `rows_written` remains exact.

## Opt-in atomic merge

`atomic_copy_on_write` MUST provide immediate physical uniqueness. It writes incoming package rows
to a target-compatible staging table, validates non-null unique merge keys and target-key
unambiguity, builds a complete merged table with a server-side anti-join plus bulk insert, verifies
the result, and publishes it with one `EXCHANGE TABLES` operation. It MUST report exact inserted and
updated counts.

This mode requires an Atomic database, a non-replicated MergeTree-family target, no dependent
materialized views, and capability-proven metadata exchange. It MUST preserve the target's engine,
sorting/partition keys, projections, codecs, and schema. The target is fully copied, so this mode
trades extreme write amplification and latency for immediate atomic visibility. Mutations,
lightweight deletes, sequential renames, and `TRUNCATE` plus insert are forbidden substitutes.

The exchanged object MUST carry an immutable package marker, including for a zero-row merge.
Recovery MUST distinguish an incoming stage, a complete unpublished merged stage, and an already
published target and continue create-or-verify without exposing a partial target.

## Append and replace

Append requires a MergeTree-family target and capability-proven deterministic insert
deduplication. Replicated and non-replicated engines are admitted only within their proven token
and acknowledgement behavior.

Replace builds and fully verifies a target-compatible staging object, then performs one atomic
metadata exchange. It requires an Atomic database and rejects replicated targets without a
cluster-wide exchange proof. The exchanged target carries an immutable package marker even for
zero-row packages.

## Mapping and limits

The mapping MUST prefer exact native booleans, signed/unsigned integers, floats, decimals within
declared precision, strings/fixed strings, dates, timestamps, arrays, tuples, maps, nullable,
low-cardinality, enums, UUID, and IP types. Timezone and decimal scale are explicit authority.
Unsupported Arrow unions, run-end encoding, and values outside native domains fail before mutation;
there is no silent JSON/string fallback. `_cdf_*` identifiers are reserved for framework fields.

Concurrency is injected and bounded. The connector MAY overlap independent encoding or server
work only within its sheet, memory leases, and measured useful-writer limits and MUST join all work
before settlement. Cancellation stops new writes while preserving deterministic verify/redrive
evidence.

## Acceptance criteria

- Default keyed merge accepts only a capability-proven ReplacingMergeTree layout, writes through
  direct ArrowStream, and verifies the exact logical winners with `FINAL`.
- Planning and receipt output make eventual physical uniqueness explicit; a non-`FINAL` duplicate
  is never represented as immediate uniqueness.
- Atomic merge publishes either the complete old target or complete merged target, including
  zero-row packages, and exact inserted/updated counts remain independently verifiable.
- Merge-key nullability, absence, order drift, partition drift, duplicate package keys, ambiguous
  target keys, dependent materialized views, and unsupported engines fail before publication.
- Append/replace settlement, replay, crash recovery, redaction, mapping, catalog, jobs-invariance,
  and connector certification laws pass.
- The destination macro benchmark reaches the 0.90 direct ArrowStream roofline in the default
  native merge cell and append cell using identical production acknowledgement/token settings.

## Explicit exclusions

Silent engine conversion, version-column guessing, cross-partition replacement without proof,
eventual compaction as the receipt itself, implicit `FINAL` injection into user queries,
unacknowledged async inserts, generic transaction claims, arbitrary mutations, private wire
protocols, and URI query configuration are excluded.
