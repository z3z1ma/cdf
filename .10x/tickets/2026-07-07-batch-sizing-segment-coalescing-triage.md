Status: open
Created: 2026-07-07
Updated: 2026-07-11
Parent: .10x/tickets/2026-07-07-performance-investigation-backlog.md

# Triage batch sizing and segment coalescing

## Scope

Investigate whether CDF needs explicit batch-size and package-segment coalescing policy to avoid many tiny Arrow IPC segment files or overly large memory-resident batches.

This ticket is triage only. It does not authorize changing batch identity, package segment layout, resource contracts, writer behavior, or deterministic package hashing.

## Current hypothesis

CDF's performance will depend heavily on the size and count of `RecordBatch` payloads and package segments. Very small batches amplify manifest, hash, filesystem, and destination commit overhead. Very large batches increase latency, memory pressure, retry cost, and error blast radius. Current behavior may mostly inherit source batch sizes rather than applying a coherent policy.

## Investigation questions

- What batch sizes do current resource paths produce: file preview, native Parquet, declarative REST, subprocess, Python, and future SQL?
- Does `cdf-engine` write one package segment per output batch, and is that appropriate?
- Should the engine coalesce small batches into target-size segments, or should resources own batch sizing?
- How does segment coalescing affect source positions, cursor advancement, lineage, replay, and error reporting?
- What target rows/bytes per batch or segment should be defaults, if any?
- Should batch-size policy be part of resource capabilities, project config, package metadata, or internal execution profile only?

## Candidate validation scenarios

- Many tiny REST pages with one row each.
- Many small local files with small batches.
- One large Parquet file with naturally large row groups.
- Wide schema where byte size matters more than row count.
- Cursor positions where coalescing multiple source batches into one package segment must preserve output position semantics.

## Acceptance criteria

- Inventory current batch-to-segment mapping and source batch-size behavior by implemented resource type.
- Identify whether tiny segments are already a practical performance problem or only a theoretical concern.
- State the semantic rule for source positions when coalescing batches.
- Recommend no action, documentation, resource-specific batch sizing, engine-level coalescing, or package writer changes.
- If implementation is recommended, open a focused ticket that preserves deterministic package hashes for fixed input and records coalescing decisions in package metadata if needed.

## Evidence expectations

- Source inspection of `Batch`, `ResourceStream`, engine execution, package builder, and implemented resource runtimes.
- Package fixture inspection to count segments and sizes for existing golden/local runs if available.
- Optional measurement of many-small-segment overhead under `.10x/tickets/2026-07-07-package-io-hashing-overhead-triage.md`.

## Explicit exclusions

No batch-size configuration surface, no engine coalescing implementation, no package segment semantic change, no source-position change, no golden hash update, and no conformance change before triage.

## References

- `.10x/tickets/2026-07-07-performance-investigation-backlog.md`
- `.10x/specs/resource-authoring-planning-batches.md`
- `.10x/specs/package-lifecycle-determinism.md`
- `crates/cdf-kernel/src/batch.rs`
- `crates/cdf-engine/src/execution.rs`
- `crates/cdf-package/src/builder.rs`

## Progress and notes

- 2026-07-07: Opened from performance discussion. The suspected performance risk is not Arrow itself, but pathological batch/segment granularity amplifying otherwise reasonable fixed costs.
- 2026-07-11: P3 source audit confirmed the hypothesis: formats default to 1,024 rows, REST follows page size, and engine execution writes one global encounter-order segment per accepted batch. Ratified the adaptive-microbatch/canonical-segment split and assigned implementation to `.10x/tickets/2026-07-11-p3-a3-canonical-segmentation-adaptive-batching.md`. This triage remains open until WS-L supplies measurements and A3 records the before/after closure evidence.

## Blockers

None for investigation. Implementation is blocked on a ratified source-position/coalescing contract.
