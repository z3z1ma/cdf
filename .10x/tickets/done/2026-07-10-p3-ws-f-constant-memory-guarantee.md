Status: done
Created: 2026-07-10
Updated: 2026-07-25
Parent: .10x/tickets/done/2026-07-10-p3-terabyte-scale-program.md
Depends-On: .10x/tickets/done/2026-07-11-p3-a2-unified-memory-ledger.md, .10x/specs/performance-lab-and-envelope.md, .10x/specs/runtime-memory-backpressure.md

# P3 WS-F: constant-memory guarantee

## Scope

Make the memory law executable: generated 100 GB input under 2 GiB, peak-RSS assertion, spill observation, successful completion, too-small-budget clean failure, `cdf doctor` budget reporting, and P1 run-panel peak ledger rendering.

## Activated children

- `.10x/tickets/done/2026-07-11-p3-f1-budget-enforcement-headroom.md`
- `.10x/tickets/done/2026-07-11-p3-f2-materialization-closure-audit.md`
- `.10x/tickets/done/2026-07-11-p3-f3-stress-generators-laws.md`
- `.10x/tickets/done/2026-07-11-p3-f4-one-tb-memory-closeout.md`

## Acceptance criteria

- RSS remains within the ratified ceiling independent of input size.
- Keyless dedup, decompression, decoder windows, queues, package builders, and destination staging are ledger-accounted or spilled.
- A budget too small for one legal batch fails with a remedial `Data` error, never OOM.
- Stress and failure laws are permanent slow-tier CI.

## Blockers

None. All four children are terminal.

## Progress

- 2026-07-14: F2 isolated and bounded DuckDB's package-long native transaction, cutting the 2.15 GB FineWeb run's peak footprint from about 3.25 GB to 1.39 GB while retaining 85.8% of uncapped local throughput. DuckDB scratch capacity now reserves against the shared spill authority. This is a child milestone, not the parent constant-memory proof; F1–F4 remain active/open.
- 2026-07-19: F1 closed process-tree enforcement and calibrated reporting. A clean 41,169,720-row EC2 TLC-to-DuckDB run completed in `10.477s` under a 6 GiB cgroup with child RSS and aggregate cgroup peaks reported separately and zero memory events; product JSON/human diagnostics and an actual bounded Python child case satisfy the authority/reporting slice. F3/F4 retain the synthetic 100 GB/2 GiB stress-and-spill law.
- 2026-07-25: F3 closed the 100 GiB / 2 GiB and adversarial stress matrix. The exact product run
  completed at 1.658 GiB peak RSS with verified evidence, and 5/20/100 GiB geometric observations
  showed no input-size slope. Forced spill, exhausted spill, impossible budget, compressed remote,
  slow-consumer, metadata, dedup, quarantine, staged-writer, and foreign-child laws are terminal.
  F4 retains the final 1 TiB/default-budget scale and permanent slow-tier integration.
- 2026-07-25: F4 closed the exact default-policy scale cell. The complete governed path processed
  1.0086 TiB, 5.436 billion rows, 1,024 files, and 5,120 segments at 1.896 GiB peak process RSS
  under CDF's unoverridden 4 GiB process policy; package, receipt, and checkpoint verification
  succeeded and the enclosing cgroup recorded zero OOM. Cold discovery over all 1,024 Parquet
  footers completed at 106,496 KiB peak RSS. The generated allocation-owner matrix has no open
  row, its evidence paths are validated in slow quality, and `QUALITY.md` preserves the 100 GiB
  and 1 TiB dedicated-host procedures without returning giant fixtures to hosted CI.

## References

- `.10x/decisions/process-tree-constant-memory-proof.md`
- `.10x/specs/constant-memory-proof.md`

## Evidence

- F1: `.10x/tickets/done/2026-07-11-p3-f1-budget-enforcement-headroom.md`
- F2: `.10x/tickets/done/2026-07-11-p3-f2-materialization-closure-audit.md`
- F3: `.10x/evidence/2026-07-25-p3-f3-constant-memory-matrix.md`
- F4: `.10x/evidence/2026-07-25-p3-f4-one-tib-closeout.md`
- Permanent gate and ownership matrix: `QUALITY.md`, `.github/workflows/slow-quality.yml`,
  `docs/memory-allocation-owners.md`, and `tools/generate-memory-owner-matrix.py`

## Review

Verdict: pass.

The child evidence covers process-tree enforcement, native materialization boundaries, geometric
input-size falsification, forced spill/exhaustion, compressed and slow-consumer backpressure,
foreign children, metadata/cardinality, the exact 1 TiB/default-policy cell, and a permanent
dedicated-host procedure. No critical or significant finding remains. The terminal evidence does
not claim constant metadata at million-file or unbounded-stream horizons; that distinct future
boundary is recorded in the active roadmap.

## Retrospective

Constant memory must be proved at three separate layers: the managed ledger, foreign/native
process ownership, and actual process-tree RSS. Treating one as a proxy for the others caused the
early audit churn. The durable matrix now names each owner and the exact authority that bounds or
measures it, while the scale runner makes semantic success, receipt verification, cleanup, and RSS
part of the same law. Large stress fixtures belong on controlled dedicated hosts; only the cheap
failure law and owner-matrix closure check belong in hosted slow CI.
