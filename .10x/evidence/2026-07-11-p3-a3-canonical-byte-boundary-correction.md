Status: recorded
Created: 2026-07-11
Updated: 2026-07-11
Relates-To: .10x/tickets/done/2026-07-11-p3-a3-canonical-segmentation-adaptive-batching.md, .10x/specs/canonical-segmentation-adaptive-batching.md

# P3 A3 canonical byte-boundary correction

## What was observed

Inspection found that `canonical-segmentation-v1.target_bytes` participated in plan identity but the assembler never consulted it. The previous hard-byte check applied to each incoming Arrow chunk independently, so accumulated segment memory could exceed the declared maximum. Position joining also happened before a size-triggered flush, allowing the next batch's cursor to be attached to a segment that did not contain that batch.

The assembler now uses slice-logical Arrow bytes to select the largest row prefix within the remaining canonical byte target, tracks cumulative logical bytes, flushes on either deterministic row or byte target, and rejects a single logical row above the hard maximum. Positioned batches remain unsplittable without exact slice-position authority. Top-level nullable validity cost is normalized per row so absent/present bitmap allocation and source rechunking do not alter the estimate.

## Procedure

1. Added byte-target rechunking equivalence over independently allocated UTF-8 batches.
2. Added nullable logical-byte additivity across one batch versus two batches.
3. Added a regression for cursor authority across a target-triggered flush.
4. Ran `cargo test -p cdf-engine segmentation::tests -- --nocapture`: eight passed.
5. Ran `cargo clippy -p cdf-engine --all-targets -- -D warnings`: passed.
6. Ran the complete `cdf-engine` library suite after the full Arrow estimator landed: 80 passed, one ignored release benchmark, zero failures. The fixed package hash remained `sha256:d5c6b049a9986db182491627af42f74c83cfa763f21a9cad28e9d677001a5959`.
7. Ran the release fixed-cost package benchmark over 65,536 `int64` rows: the legacy 64 × 1,024-row segment path took 576,863,166 ns; one canonical 64k segment took 42,889,583 ns, a 13.45x speedup on this host.

## What this supports or challenges

This supports plan-effective row/byte segmentation for primitive, variable-width/view, list/large-list/fixed-size-list/list-view, struct, map, union, and dictionary arrays and fixes position authority at flush boundaries. Nested accounting explicitly slices child value ranges instead of counting an entire shared child buffer for every parent slice. Dictionary value sizes are cached by dictionary index so repeated values do not repeat nested size traversal.

## Limits

A5 owns transferring already-accounted envelopes into assembler retention so physical shared-buffer ownership is not reconstructed from per-batch estimates. C2 owns running the same canonical writer under actual concurrent jobs and making jobs=1/N a cross-archetype permanent law; A3 establishes the plan-stable segmentation function that law exercises.
