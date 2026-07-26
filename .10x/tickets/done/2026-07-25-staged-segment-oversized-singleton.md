Status: done
Created: 2026-07-25
Updated: 2026-07-25

# Admit a legal oversized staged segment as a singleton

Parent: `.10x/tickets/2026-07-25-stabilization-steady-state-program.md`

## Scope

Repair the generic live staged-ingress boundary so a segment already admitted by the
managed-memory authority is not rejected merely because its retained Arrow footprint exceeds a
destination's concurrent in-flight byte window. Such a segment must run as an exclusive
singleton; the destination window continues to bound ordinary concurrent ownership.

## Non-goals

- No destination-specific exception.
- No increase to canonical segment defaults or adapter tuning constants.
- No weakening of the managed-memory budget.
- No change to package or segment identity.

## Acceptance Criteria

- A segment larger than the destination concurrency window but no larger than the managed-memory
  budget is admitted only while no other staged bytes are owned.
- Ordinary segments retain the existing byte-bounded concurrency behavior.
- A segment larger than the managed-memory budget still fails before destination mutation.
- Finalized-package decoding remains independently bounded because its Arrow memory has not yet
  been admitted by the ledger; the repair does not inflate that path's decode window.
- Focused tests, formatting, and strict Clippy pass.
- The hosted FineWeb-to-DuckDB run that exposed the defect proceeds without a tuning override.

## References

- `.10x/specs/streaming-destination-ingress.md`
- `.10x/specs/runtime-memory-backpressure.md`
- `.10x/specs/streaming-operator-graph.md`
- `.10x/tickets/done/2026-07-11-p1-cx4-cli-conformance-performance.md`

## Assumptions

- Record-backed: the shared memory coordinator is the authority for retained Arrow memory.
- Record-backed: destination scheduling bytes describe bounded concurrent ownership.
- Record-backed: canonical segments may retain more Arrow memory than their logical byte target.

## Journal

- 2026-07-25: Activated after the hosted CX4 FineWeb run produced a legal 276,210,169-byte
  retained segment under the admitted managed-memory budget but failed against DuckDB's
  268,435,456-byte concurrent in-flight window. The failure occurred before destination mutation.
- 2026-07-25: Replaced the per-item rejection with the generic byte-channel singleton law. An
  item above the concurrent window waits until current ownership is zero, owns the window
  exclusively, and releases it through the existing RAII permit. Ordinary items retain the
  existing additive window. The preexisting managed-memory check still rejects a segment above
  the process budget before submission.
- 2026-07-25: Hosted release validation at clean revision `28765dbf` completed the exact default
  2.147 GB FineWeb-to-DuckDB run in 23.22 seconds: 1.1 million rows, 14 segments, verified
  receipt, committed checkpoint, 6,545,948 KiB peak RSS, and zero swap.

## Blockers

None.

## Evidence

- `cargo fmt --all -- --check` — passed.
- Focused oversized-singleton concurrency regression — passed.
- `CARGO_BUILD_JOBS=12 cargo test -p cdf-project --lib` — 215 passed, zero failed.
- `CARGO_BUILD_JOBS=12 cargo clippy -p cdf-project --all-targets -- -D warnings` — passed.
- `.10x/evidence/2026-07-25-cli-hosted-conformance.md` — exact real-product regression proof.

## Review

Pass. Fresh inspection found no destination identity branch, cap inflation, package-identity
change, or ordinary-path scheduling change beyond one predictable comparison. The singleton may
exceed the destination concurrency window only after the managed-memory authority has already
admitted its exact retained bytes and only while no other staged bytes are owned. Finalized
package decoding remains independently bounded because its Arrow allocation has not yet been
admitted.

## Retrospective

Byte-bounded channels need an explicit indivisible-message law. Treating a concurrency target as
a per-message validity limit turns harmless representation overhead into a default-path failure.
The correct hierarchy is: the memory ledger decides whether one retained payload is legal; the
destination window decides how much admitted work may overlap.
