Status: open
Created: 2026-07-11
Updated: 2026-07-11
Parent: .10x/tickets/2026-07-10-p3-ws-f-constant-memory-guarantee.md
Depends-On: .10x/tickets/2026-07-10-p3-ws-l5-preoptimization-baseline.md, .10x/tickets/2026-07-11-p3-a2-unified-memory-ledger.md, .10x/specs/constant-memory-proof.md

# P3 F1: process-tree enforcement and headroom calibration

## Scope

Implement portable process-tree memory observation, Linux cgroup v2 enforced provider, child-budget propagation, host/runtime/native headroom calibration, exact default/effective budget resolution, and doctor/explain reporting.

## Acceptance criteria

- At least one release host enforces/records cgroup plus process-tree RSS/managed peaks without conflating metrics.
- Exact headroom/default resolution is evidence-backed/versioned; unsafe/small budgets fail before work.
- Python/subprocess children remain inside aggregate authority or enforced sub-budgets.
- Doctor/run JSON and human output report all authorities/caveats accurately.

## Evidence expectations

Host/provider fixtures, calibration reports, cgroup OOM/event tests, child memory cases, cross-platform unavailable labels, redaction, and adversarial metric review.

## Explicit exclusions

No data-plane materialization removal or allocator adoption without separate evidence.

## Blockers

Depends on L5 and A2.

## References

- `.10x/decisions/process-tree-constant-memory-proof.md`
- `.10x/research/2026-07-11-constant-memory-proof-audit.md`
- `.10x/specs/constant-memory-proof.md`
