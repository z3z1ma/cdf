Status: superseded
Created: 2026-07-11
Updated: 2026-07-11

# Adaptive microbatches and deterministic canonical segmentation

## Context

One-source-batch-per-segment makes current packages and throughput depend on arbitrary adapter chunking. P3 requires adaptive batching and parallelism while invariant I3 requires identical package identity for fixed plan/input regardless of scheduling. Live pressure cannot safely choose identity-participating boundaries.

## Decision

CDF distinguishes execution microbatches from canonical package segments.

Execution microbatches are internal accounted Arrow chunks used by decode, transform, validation, and encoding. Their target may adapt within plan bounds based on row width, memory pressure, spill, source capability, and downstream throughput. Their sizes/durations are event/lab telemetry outside package identity. Rebatching MUST NOT change row order, values, verdicts, lineage meaning, or source-position authority.

Canonical segmentation is a deterministic function recorded in the plan and package artifact version. It includes partition ordering/namespace, row and byte targets, oversize policy, position-join policy version, and splitting capability. Once planned, pressure/scheduling cannot alter it.

Canonical segments:

- never cross logical partition boundaries;
- consume admitted rows in canonical partition/row order after contract and package-scoped dedup semantics;
- coalesce micro/source batches deterministically until the next legal boundary under the recorded policy;
- split only when the source/batch exposes exact slice-position authority;
- flush before a position transition that the typed position algebra cannot join;
- derive ids from plan partition ordinal and canonical segment ordinal, never global arrival order.

The initial target policy must be calibrated after WS-L within 8k-64k rows and 1-32 MiB retained/encoded targets. Exact constants and width estimation are versioned plan data, not hidden globals. An input chunk larger than the legal working set must be split by declared authority, spilled/redecoded through a bounded path, or fail cleanly before allocation; “emit arbitrarily oversized” is not accepted.

Manifest segment entries and state-position evidence are the replay boundary. Replay reads recorded canonical segments and does not rederive live microbatch decisions. Pressure telemetry and wall-clock facts remain in the run event/lab path, not identity-participating package trace/stats.

Package-wide first-occurrence dedup uses canonical `(partition ordinal, row ordinal)` order. Parallel algorithms may shard/spill work but must produce the same admitted order and segments as jobs=1.

## Alternatives considered

- Record live adaptive sizes in package identity: rejected because timing/scheduling would change hashes.
- Fix all sources at 64k rows: rejected because wide/nested rows, page boundaries, and memory budgets require byte awareness.
- Preserve source batches as segments: rejected because adapter/network chunking is not artifact semantics and amplifies fixed costs.
- Let the writer coalesce across files/partitions: rejected because it weakens retry, manifest, and position ownership.
- Use global atomic segment counters under parallelism: rejected because arrival order is nondeterministic.

## Consequences

The engine gains a canonical segment assembler and typed position accumulator. Source adapters expose working-set and optional exact slicing capability rather than owning package segmentation. Package artifact/golden hashes will intentionally migrate once under an explicit format-version/golden gate. P3 compares internal microbatch tuning without destabilizing packages.
