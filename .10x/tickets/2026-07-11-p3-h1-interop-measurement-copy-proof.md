Status: open
Created: 2026-07-11
Updated: 2026-07-11
Parent: .10x/tickets/2026-07-10-p3-ws-h-interop-boundaries.md
Depends-On: .10x/tickets/done/2026-07-10-p3-ws-l5-preoptimization-baseline.md, .10x/tickets/2026-07-11-p0-ix1-neutral-foreign-stream-contract.md

# P3 H1: interop measurement and copy-proof harness

## Scope

Implement common cold/warm startup, first-batch, steady-state, batch curve, CPU/allocation/copy, memory, wait, cancellation, and native-reference measurement for foreign transfer modes, including a falsifiable Arrow C zero-copy probe.

## Acceptance criteria

- Results separate Arrow C, IPC, and row modes and label unavailable/unknown cells.
- Zero-copy is asserted only when payload address/lifetime/allocation probes pass for that type path.
- Host/interpreter/protocol/build and timed/setup regions are recorded.
- Harness integrates with WS-L baselines/envelope without slowing fast checks.

## Evidence expectations

Raw machine reports, proof fixtures, calibration/repeatability, unsupported-type downgrade cases, and bias review.

## Explicit exclusions

No adapter optimization.

## Blockers

Blocked on L5 and IX1.

## References

- `.10x/specs/foreign-stream-interop.md`
- `.10x/specs/performance-lab-and-envelope.md`
