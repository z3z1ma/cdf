Status: recorded
Created: 2026-07-11
Updated: 2026-07-11
Relates-To: .10x/tickets/2026-07-11-p3-a5e-streaming-graph-integration.md, .10x/specs/canonical-segmentation-adaptive-batching.md, .10x/specs/streaming-operator-graph.md

# Zero-copy canonical microbatches

## What was observed

Canonical segment persistence previously concatenated every segment's Arrow batches into one large `RecordBatch` before IPC encoding. On January TLC this copied approximately 425 MiB of Arrow data after decode/validation solely to erase source batch boundaries. The concat was outside phase telemetry and required a conservative retained-input-plus-output memory reservation.

Canonical segments now carry the plan's fixed microbatch row/byte authority. Persistence deterministically rechunks fragments to those boundaries. A batch already matching a boundary is moved directly into the IPC writer and staged destination reader with shared Arrow buffers; only multiple fragments crossing one canonical boundary use Arrow concat. A segment may contain multiple canonical microbatches without exposing source decoder boundaries.

## Procedure

- The dedicated unit test proves an exact canonical batch retains the same Arrow column allocation (`Arc::ptr_eq`) and fragmented input coalesces to the same values.
- `cargo test -p cdf-engine --lib` passed 87 active tests; four performance/stress tests were ignored by policy.
- Source-rechunk package identity, fused/unfused identity, durable publication hook, generic staged replay, and ordinary durable-publish/final-binding tests passed.
- Strict all-target/all-feature Clippy passed for `cdf-engine` and `cdf-project`.
- Three fresh release TLC runs measured wall 2.39/1.55/1.79 seconds and CPU 1.60/1.62/1.59 seconds. Host wall variance is retained rather than normalized away.
- Direct phase telemetry on the 1.55-second sample recorded package execution at 1.211680333 seconds versus 1.240293250 seconds immediately before the change, a 2.3% reduction. Median CPU fell from roughly 1.65 to 1.60 seconds, about 3.0%.

## What this supports or challenges

This removes a package hot-path copy without weakening deterministic identity. Canonicality belongs to plan-owned microbatch boundaries, not to one monolithic batch per segment. The durable-segment and staged-ingress contracts now carry bounded batch streams, which also generalizes to large segments and destinations that naturally consume Arrow batches.

## Limits

The retained-input-plus-output reservation remains conservative even when every microbatch takes the zero-copy path; a later memory-admission refinement should preplan actual boundary concat scratch. IPC encoding and fsync remain sequential within the resource execution loop, and partition/segment parallelism remains active P3 work.
