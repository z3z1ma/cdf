Status: active
Created: 2026-07-11
Updated: 2026-07-15
Parent: .10x/tickets/2026-07-10-p3-ws-f-constant-memory-guarantee.md
Depends-On: .10x/tickets/done/2026-07-11-p3-f3-stress-generators-laws.md, .10x/tickets/done/2026-07-11-p3-c4-jobs-invariance-scaling-matrix.md, .10x/tickets/done/2026-07-11-p3-d4-parquet-streaming-writer.md

# P3 F4: 1 TB constant-memory and scaling closeout

## Scope

Run the scheduled/manual 1 TB synthetic glob-to-Parquet scenario under default budget, attach memory/performance/profile evidence, publish the owner/stress matrix, and make the enforced laws permanent slow-tier gates.

NOTE: we do not have 1TB of disk on this laptop. You need to provision a machine in AWS temporarily with enough disk space.

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

None. F3, C4, and the Parquet streaming destination are complete. The retained EC2 host has a
tuned 2 TiB root volume and sufficient capacity for the scheduled product run.

## References

- `.10x/decisions/process-tree-constant-memory-proof.md`
- `.10x/specs/constant-memory-proof.md`

## Journal

- 2026-07-25: Activated after F3 closed the 100 GiB / 2 GiB and adversarial matrix. The retained
  `c7i.4xlarge` exposes 16 logical CPUs and a tuned 2 TiB gp3 root volume with 1.9 TiB free.
  The 1 TiB law will use 1,024 deterministic files representing exactly 1 GiB each, run with no
  CDF memory override inside `MemoryMax=5G`/`MemorySwapMax=0`. The default resolver therefore keeps
  its exact 4 GiB process authority while the enclosing cgroup retains enforcement headroom.
- 2026-07-25: Extended the existing product stress runner with an explicit `default` mode that
  omits `--memory-budget` rather than laundering the configured 4 GiB value into a default-policy
  claim. F3's omitted argument still means its exact 2 GiB law; no existing invocation changes.
