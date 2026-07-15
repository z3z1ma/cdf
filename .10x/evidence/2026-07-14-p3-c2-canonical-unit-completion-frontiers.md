Status: recorded
Created: 2026-07-14
Updated: 2026-07-14

# Canonical unit completion publishes no-lookback frontiers

## Observation

The generic canonical stream frontier can now publish one completion callback per decode unit, only after that unit reaches EOF in canonical order. Registered format execution joins this lifecycle with the codec's complete byte envelopes and calls the neutral `ByteSource::release_before` hook. Sequential and parallel unit paths use the same derived frontier semantics.

No source is required to retain bytes: the default release hook is a no-op. Decorators that preserve byte offsets forward the signal. A future bounded spool may physically reclaim storage only in its own implementation; orchestration never branches on source or format identity.

## Procedure

The permanent canonical-frontier test opens four streams with capacity three, collects their values, and requires completion callbacks exactly once in `[0, 1, 2, 3]` order. Existing stalled-head and later-error laws continue to pass. `cdf-source-files` compiles all targets with the sequential path releasing after decoded EOF and the parallel path releasing from the canonical frontier callback.

Commands and results:

- `CARGO_BUILD_JOBS=12 cargo test -p cdf-runtime canonical_frontier --locked`: 3 passed.
- `CARGO_BUILD_JOBS=12 cargo check -p cdf-source-files --all-targets --locked`: passed.
- `cargo fmt --all` and focused diff inspection: passed.

## What this supports or challenges

This supports `.10x/specs/remote-local-io-overlap.md`'s requirement that progressive eviction consume a codec-session monotone no-lookback frontier. It also strengthens C2's file-unit completion authority: release happens after canonical EOF, not when a unit is merely opened, scheduled, or produces its last currently buffered batch.

## Limits

No current source physically evicts bytes in this slice. Global nested admission, partition-level completion, retry/reattest, live cancellation chaos, and jobs-invariance closeout remain active C2/G2 work.
