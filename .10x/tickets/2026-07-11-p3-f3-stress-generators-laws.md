Status: open
Created: 2026-07-11
Updated: 2026-07-11
Parent: .10x/tickets/2026-07-10-p3-ws-f-constant-memory-guarantee.md
Depends-On: .10x/tickets/done/2026-07-11-p3-f1-budget-enforcement-headroom.md, .10x/tickets/done/2026-07-11-p3-f2-materialization-closure-audit.md

# P3 F3: constant-memory generators and stress laws

## Scope

Build bounded deterministic generators and execute geometric-size, 100 GB/2 GiB, too-small, spill-full, metadata, compression, dedup, quarantine, slow destination, remote, and foreign-child stress cases with semantic assertions.

## Acceptance criteria

- Generator/setup memory is separate and bounded.
- 100 GB completes under enforced 2 GiB process-tree RSS budget with observed spill and no OOM event.
- Geometric inputs show no memory slope; repeated runs show no leak/fragmentation drift.
- Below-minimum and spill-full cases fail cleanly with exact remediation.
- Every case verifies package/receipt/checkpoint semantics where applicable.

## Evidence expectations

Machine reports/raw high-water/cgroup/ledger/spill data, package verification, failure-mode output, soak curves, host labels, and adversarial workload review.

## Explicit exclusions

No committed giant datasets.

## Blockers

Depends on F1/F2.

## References

- `.10x/specs/constant-memory-proof.md`
- `.10x/specs/performance-lab-and-envelope.md`
