Status: done
Created: 2026-07-11
Updated: 2026-07-25
Parent: .10x/tickets/done/2026-07-10-p3-ws-f-constant-memory-guarantee.md
Depends-On: .10x/tickets/done/2026-07-11-p3-f3-stress-generators-laws.md, .10x/tickets/done/2026-07-11-p3-c4-jobs-invariance-scaling-matrix.md, .10x/tickets/done/2026-07-11-p3-d4-parquet-streaming-writer.md

# P3 F4: 1 TB constant-memory and scaling closeout

## Scope

Run the scheduled/manual 1 TB synthetic glob-to-Parquet scenario under default budget, attach memory/performance/profile evidence, publish the owner/stress matrix, and make the enforced laws permanent slow-tier gates.

NOTE: we do not have 1TB of disk on this laptop. You need to provision a machine in AWS temporarily with enough disk space.

## Acceptance criteria

- 1 TB completes under default process budget with stable RSS, correct spill/cleanup, verified package/destination receipt, and scaling until device saturation.
- The 100 GB enforced law is a permanent dedicated-host scheduled/manual gate; the too-small law
  remains in slow CI. Giant scale cells do not enter pull-request or GitHub-hosted fast checks.
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
- 2026-07-25: Made the scale boundary permanent without re-bloating fast or GitHub-hosted CI.
  `QUALITY.md` now gives the exact dedicated-host 100 GiB/2 GiB and 1 TiB/default invocations,
  required semantic/RSS/cgroup assertions, and the explicit fixture-timing boundary. The generated
  allocation-owner checker now rejects missing/unsafe evidence paths and exposes
  `--require-closed`; scheduled slow quality invokes it so a future unclassified row cannot drift
  back into the matrix unnoticed.
- 2026-07-25: Reconciled the matrix's stale post-F2 rows against current code and evidence. Native
  clients/codecs retain declared bounded windows plus named measured headroom; metadata whose
  current artifact contract remains cardinality-bearing is labeled measured rather than falsely
  called constant-size. The 1 TiB cell is the terminal evidence for the 1,024-file/5,120-segment
  package, position, receipt, and checkpoint envelope. A warm real PostgreSQL binary-COPY control
  processed 524,288 rows at 1.80 million rows/s with a 164.9 MB command high-water and 3.00x CSV
  throughput. The generated matrix has no `open` row and its closure check passes, conditional on
  the still-running 1 TiB cell reaching terminal verification.
- 2026-07-25: The exact dedicated-host run completed 1,108,930,093,056 logical bytes
  (1.0086 TiB), 5,435,817,984 rows, 1,024 files, and 5,120 canonical segments in 2,694.863
  seconds. Peak process RSS was 2,035,298,304 bytes under the unoverridden 4 GiB policy;
  managed peak was 2,490,367,380 of 3,650,722,202 bytes and all managed ownership returned
  to zero. The enclosing 5 GiB cgroup recorded no OOM or OOM-kill. The package hash, Parquet
  receipt, and checkpoint committed, and a second verifier checked 5,135 identity files.
  Streaming required no algorithmic spill; F3 remains the forced-spill/exhaustion authority.
- 2026-07-25: A separate cold discovery over the 1,024-file inventory completed in 0.36 seconds
  at 106,496 KiB peak RSS using exhaustive file coverage and Parquet format metadata. It
  transferred 34,863,104 footer bytes, selected every file, and wrote no project or execution
  artifact. This directly closes discovery-evidence cardinality for the measured envelope.
- 2026-07-25: Fresh-hat closure review traced the runner's semantic assertions, generated owner
  matrix, raw terminal report, independent package verifier, discovery report, and accepted C4/D8
  scaling curve. No critical or significant finding remains. The run's cgroup `max` events are
  page-cache reclaim under the outer enforcement envelope, not process RSS growth or OOM.

## Evidence

- 1 TiB/default-budget completion, stable RSS, package/receipt/checkpoint correctness:
  `.10x/evidence/2026-07-25-p3-f4-one-tib-closeout.md` and its retained raw reports.
- Spill and too-small-budget behavior:
  `.10x/evidence/2026-07-25-p3-f3-constant-memory-matrix.md`.
- Scaling until the measured device/concurrency knee:
  `.10x/evidence/2026-07-14-p3-c4-fineweb-local-scaling.md`,
  `.10x/evidence/2026-07-15-p3-d8-parquet-staged-ingress.md`, and the C4/D8 terminal tickets.
- Permanent gate and operator procedure:
  `QUALITY.md`, `.github/workflows/slow-quality.yml`, and
  `tools/run-constant-memory-stress.sh`.
- Allocation classification:
  `docs/memory-allocation-owners.md` generated from
  `tools/memory-owner-classifications.json`; `--check --require-closed` passes with zero open row.

## Review

Verdict: pass.

No critical or significant finding remains. The evidence is deliberately bounded: hard-linked
repeated input validates logical work, lifecycle, memory, and measured metadata cardinality, not
unique-byte cold-storage throughput. Current identity arrays remain cardinality-bearing and are
proven through 1,024 files/5,120 segments only. The roadmap records the later extreme-cardinality
externalization boundary without inventing an active implementation requirement.

## Retrospective

The useful separation was memory law versus throughput roofline. One long 1 TiB run can prove the
former but cannot honestly replace the accepted same-path C4/D8 scaling curve for the latter.
Similarly, zero spill is the correct result for a healthy streaming path; forcing spill again at
1 TiB would add cost without evidence, because F3 already falsifies spill and exhaustion behavior.
The owner matrix had also drifted behind current code, so closure now validates source discovery
and every retained evidence path rather than allowing prose classification to age silently.
