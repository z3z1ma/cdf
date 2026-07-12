Status: open
Created: 2026-07-11
Updated: 2026-07-11
Parent: .10x/tickets/2026-07-10-p3-ws-a-streaming-runtime-pipeline.md
Depends-On: .10x/tickets/done/2026-07-10-p3-ws-l5-preoptimization-baseline.md, .10x/tickets/done/2026-07-11-p3-a2-unified-memory-ledger.md, .10x/specs/canonical-segmentation-adaptive-batching.md

# P3 A3: canonical segmentation and adaptive batching

## Scope

Implement plan-versioned segmentation policy, canonical partition/segment identifiers, position accumulation/slice capability, accounted adaptive microbatch controller, and deterministic segment assembler. Migrate one-batch-one-segment behavior through an explicit package/golden artifact version gate.

## Acceptance criteria

- WS-L evidence selects exact initial targets inside the ratified bounds and records the rationale.
- Source batch/page size, jobs, pressure, memory headroom, destination speed, and spill timing do not change canonical segment/package hashes.
- Segment ids derive from plan partition/segment ordinals; no global arrival counter remains.
- Position joins/splits are typed and conservative; unsupported algebra never invents a cursor.
- Tiny inputs coalesce, oversized inputs remain bounded, and microbatch telemetry is outside package identity.
- Package-scoped dedup retains canonical first-occurrence semantics.

## Evidence expectations

Artifact-version decision/goldens, property tests over batch rechunking and jobs, position-variant matrix, tiny/oversize benchmarks, memory accounting, package/replay compatibility, and adversarial determinism review.

## Explicit exclusions

No full multi-stage Tokio pipeline, row-group parallel reader, production dedup spill algorithm, or destination bulk implementation.

## Blockers

None. L5 baseline evidence and A2 accounted payloads are complete.

## Progress and notes

- 2026-07-11: Ratified exact `canonical-segmentation-v1` targets in `.10x/decisions/p3-initial-batch-segment-targets.md`: 64k rows/8 MiB canonical target, 64k/32 MiB hard ceiling, and 8k–64k/1–32 MiB adaptive execution bounds.
- 2026-07-11: Added validated policy and deterministic `p{partition}-s{segment}` identifier namespace, an adaptive controller driven only by observed width and non-identity memory telemetry, and conservative typed position joins. File manifests union by exact identity, numeric/timestamp cursors advance only within identical authority, compatible logs/composites join recursively, and opaque/page/foreign mismatches force a boundary.
- 2026-07-11: Added the canonical partition-local assembler. Unpositioned inputs split/coalesce at policy row boundaries independent of source rechunking; typed positions join or force flush; positioned oversize fails unless later supplied exact slice authority. Focused laws prove one large batch and four differently chunked tiny batches emit identical ids, row boundaries, and values.
- 2026-07-11: Activated the plan-artifact gate in `.10x/decisions/canonical-segmentation-plan-artifact-gate.md`. Newly compiled package-sink operators record the full policy in plan/explain identity; execution rejects absent/duplicate policies; legacy serialized plans default a missing field for readability without resegmenting recorded packages.
- 2026-07-11: Migrated production package writing to partition-local canonical assemblers, including package-scoped dedup. Segment ids now derive from plan partition/segment ordinals, tiny source batches coalesce, canonical segments contain one rechunked Arrow batch, and lineage/profile identity records canonical partitions/rows/segments rather than adapter batch ids or microbatch counts. The permanent end-to-end law proves one source page versus three differently sized pages yields byte-identical identity files, segments, lineage, and manifest hash.
- 2026-07-11: Fixed the post-migration golden package hash at `sha256:d5c6b049a9986db182491627af42f74c83cfa763f21a9cad28e9d677001a5959`. A3 remains open: A4 must inject the shared coordinator before assembler retention/concat scratch can be honestly ledger-accounted, and C1 must exercise jobs=1/N scheduling against the canonical writer.
- 2026-07-11: The injected production run path now reserves the canonical retained-input plus concat-output working set from the shared coordinator before allocation/publication and fails cleanly with remediation on exhaustion. A5 still owns converting upstream assembler retention from compatibility `RecordBatch` inputs to fully accounted envelopes.
- 2026-07-11: Corrected the canonical assembler after inspection proved the serialized byte target was not live. Flat primitive/UTF-8 segments now split on cumulative plan bytes as well as rows, normalize nullable bitmap allocation for rechunking invariance, reject over-maximum rows, and preserve cursor authority across size-triggered flushes. Evidence: `.10x/evidence/2026-07-11-p3-a3-canonical-byte-boundary-correction.md`. A3 remains active for the nested/dictionary/union/run/view type matrix and full package conformance.

## References

- `.10x/decisions/adaptive-microbatch-canonical-segmentation.md`
- `.10x/research/2026-07-11-batch-segment-determinism-audit.md`
- `.10x/specs/package-lifecycle-determinism.md`
- `.10x/tickets/2026-07-07-batch-sizing-segment-coalescing-triage.md`
