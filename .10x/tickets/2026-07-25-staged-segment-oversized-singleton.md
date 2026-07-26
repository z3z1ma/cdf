Status: active
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
- `.10x/tickets/2026-07-11-p1-cx4-cli-conformance-performance.md`

## Assumptions

- Record-backed: the shared memory coordinator is the authority for retained Arrow memory.
- Record-backed: destination scheduling bytes describe bounded concurrent ownership.
- Record-backed: canonical segments may retain more Arrow memory than their logical byte target.

## Journal

- 2026-07-25: Activated after the hosted CX4 FineWeb run produced a legal 276,210,169-byte
  retained segment under the admitted managed-memory budget but failed against DuckDB's
  268,435,456-byte concurrent in-flight window. The failure occurred before destination mutation.

## Blockers

None.

## Evidence

Pending.

## Review

Pending.

## Retrospective

Pending.
