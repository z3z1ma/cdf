Status: active
Created: 2026-07-10
Updated: 2026-07-19
Parent: .10x/tickets/2026-07-10-p3-terabyte-scale-program.md
Depends-On: .10x/tickets/done/2026-07-11-p3-a2-unified-memory-ledger.md, .10x/specs/performance-lab-and-envelope.md, .10x/specs/runtime-memory-backpressure.md

# P3 WS-F: constant-memory guarantee

## Scope

Make the memory law executable: generated 100 GB input under 2 GiB, peak-RSS assertion, spill observation, successful completion, too-small-budget clean failure, `cdf doctor` budget reporting, and P1 run-panel peak ledger rendering.

## Activated children

- `.10x/tickets/done/2026-07-11-p3-f1-budget-enforcement-headroom.md`
- `.10x/tickets/done/2026-07-11-p3-f2-materialization-closure-audit.md`
- `.10x/tickets/2026-07-11-p3-f3-stress-generators-laws.md`
- `.10x/tickets/2026-07-11-p3-f4-one-tb-memory-closeout.md`

## Acceptance criteria

- RSS remains within the ratified ceiling independent of input size.
- Keyless dedup, decompression, decoder windows, queues, package builders, and destination staging are ledger-accounted or spilled.
- A budget too small for one legal batch fails with a remedial `Data` error, never OOM.
- Stress and failure laws are permanent slow-tier CI.

## Blockers

None at the parent level. Child dependencies and closure gaps remain authoritative in F1–F4.

## Progress

- 2026-07-14: F2 isolated and bounded DuckDB's package-long native transaction, cutting the 2.15 GB FineWeb run's peak footprint from about 3.25 GB to 1.39 GB while retaining 85.8% of uncapped local throughput. DuckDB scratch capacity now reserves against the shared spill authority. This is a child milestone, not the parent constant-memory proof; F1–F4 remain active/open.
- 2026-07-19: F1 closed process-tree enforcement and calibrated reporting. A clean 41,169,720-row EC2 TLC-to-DuckDB run completed in `10.477s` under a 6 GiB cgroup with child RSS and aggregate cgroup peaks reported separately and zero memory events; product JSON/human diagnostics and an actual bounded Python child case satisfy the authority/reporting slice. F3/F4 retain the synthetic 100 GB/2 GiB stress-and-spill law.

## References

- `.10x/decisions/process-tree-constant-memory-proof.md`
- `.10x/specs/constant-memory-proof.md`
