Status: open
Created: 2026-07-11
Updated: 2026-07-15
Parent: .10x/tickets/2026-07-10-p3-ws-f-constant-memory-guarantee.md
Depends-On: .10x/tickets/2026-07-11-p3-f3-stress-generators-laws.md, .10x/tickets/done/2026-07-11-p3-c4-jobs-invariance-scaling-matrix.md, .10x/tickets/done/2026-07-11-p3-d4-parquet-streaming-writer.md

# P3 F4: 1 TB constant-memory and scaling closeout

## Scope

Run the scheduled/manual 1 TB synthetic glob-to-Parquet scenario under default budget, attach memory/performance/profile evidence, publish the owner/stress matrix, and make the enforced laws permanent slow-tier gates.

## Acceptance criteria

- 1 TB completes under default process budget with stable RSS, correct spill/cleanup, verified package/destination receipt, and scaling until device saturation.
- The 100 GB enforced law and too-small law are permanent CI slow-tier gates.
- Doctor/run memory rendering matches raw evidence.
- No materialization/unclassified allocation remains open.

## Evidence expectations

Full raw reports/profiles/memory curves, package/receipt verification, generated matrix/docs, CI workflow proof, and adversarial memory review.

## Explicit exclusions

No distributed execution claim.

## Blockers

Depends on F3; C4 and the Parquet streaming destination are complete.

## References

- `.10x/decisions/process-tree-constant-memory-proof.md`
- `.10x/specs/constant-memory-proof.md`
