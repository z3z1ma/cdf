Status: active
Created: 2026-07-26
Updated: 2026-07-26

# Performance evidence and regression triage

## Purpose

CDF treats performance and correctness as joint product requirements. This record defines what
constitutes comparable performance evidence and how to find regressions without wasting days on
unstructured tuning. The procedure is
`.10x/skills/investigate-cdf-performance-regressions/SKILL.md`.

## A number is not evidence without its cell

Every promoted measurement must identify:

- source revision and dirty-tree status;
- dependency/feature/linkage tuple;
- build profile and target CPU assumptions;
- host instance type, physical/logical CPUs, volume class, IOPS, throughput, memory, cgroup limits,
  and operating-system tuning;
- workload identity, exact rows/logical bytes/files/segments/schema width and data distribution;
- source transport and generation identity;
- destination kind, empty/pre-existing state, disposition, and persistence path;
- CDF memory budget, jobs, segment policy, codec/compression, and all tuning environment;
- warm/cold I/O mode and number of samples;
- exact timed region;
- wall, user, system, RSS, managed-memory peak, spill, runtime I/O bytes, rows, package identity,
  receipt, and checkpoint result.

Without those facts, keep the result as a smoke observation. Do not promote it into a default or
roofline claim.

## Current dated reference points

These are useful regression floors only for matching cells:

### One-TiB synthetic Parquet acceptance

Authority: `.10x/evidence/2026-07-26-parquet-parallel-one-tib-rerun.md`.

- Host: `c7i.4xlarge`, 8 physical/16 logical CPUs.
- Volume: 250-GiB gp3, 16,000 IOPS, 1,000 MiB/s.
- Logical input: 1.0086 TiB, 5.435 billion rows, 1,024 files.
- Canonical output: 5,120 segments.
- Wall: 499.07 seconds.
- Logical throughput: 2.222 GB/s.
- Row throughput: 10.892 million rows/s.
- Average CPU: 678%.
- Peak RSS: 3.923 GB under default 4-GiB managed budget.
- Managed peak: 3.163/3.650 GB and returned to zero.
- Spill: none.
- Improvement over identical prior cell: 5.40x from 411.5 MB/s and 2,694.863 seconds.

The test is highly compressible synthetic data. Logical GB/s is not physical device GB/s and must
not be relabelled as such.

### Full-year TLC to DuckDB

Controlled EC2 evidence around the retained final path is approximately 10.3–10.5 seconds for
41,169,720 TLC rows on its recorded host/fixture. This is a narrow regression gate for the same
dataset, package semantics, DuckDB state, and memory setting. Do not use it to predict a
2,000-column table or remote public URL.

### FineWeb full remote object

One live sample fetched a roughly 2.147-GB public object through the governed CDF full path in
16.21 seconds versus a 14.70-second curl transfer floor. This established approximately 1.10x
transfer-floor wall for that endpoint/revision. It is a valuable smoke for the sequential-spool
path, not a stable internet service-level objective.

Public endpoints redirect, throttle, omit metadata, expire signed URLs, and change content. A live
failure can identify a robustness problem but cannot alone prove a code regression.

## The generated envelope may lag

`docs/performance-envelope.md` is generated evidence publication, not a hand-edited authority. As
of 2026-07-26 it still reports the older 411.5-MB/s one-TiB result. The dated July 26 evidence is
newer. Refresh the generated envelope only through an owning ticket with reproducible input; do
not manually replace one number and leave the rest of the cell stale.

## Diagnose the topology before tuning

Measure where wall time lives:

```text
inventory/discovery
→ transfer/read
→ decode
→ validate/normalize
→ canonical segment encode
→ persist/hash/finalize
→ destination ingress/materialization
→ receipt verification
→ checkpoint
```

Common interpretations:

- High wall, low user CPU, low network: serialized I/O, request latency, backpressure, locks, permit
  collapse, or blocking waits.
- High user CPU with host saturation: codec, validation, hashing, allocation, or destination
  materialization is the likely roofline.
- High system CPU: copies, syscalls, decompression, page cache churn, fsync, or many small files.
- High RSS with low managed memory: an unaccounted library/destination buffer exists outside the
  ledger.
- High managed pressure and spill: batch/segment/task admission or a true budget limit.
- Fast source/package but slow run: destination/receipt/checkpoint is dominant.

Do not attribute total wall to the phase that emitted the last visible CLI line. Structured phase
events and process counters are the authority.

## Find the first bad change

When the old good result is known:

1. Create one deterministic, local, release-built end-to-end predicate.
2. Verify current bad once and a known good revision once.
3. Rank likely commits from the architectural change history.
4. Use true binary search over the bounded range, testing at least the midpoint each iteration.
5. Keep the dataset, destination state, target directory strategy, build tuple, jobs, memory, and
   timing command invariant.
6. Inspect the first bad diff and trace its control-flow effect.

Do not poke batches, jobs, caps, and environment values randomly before locating a code regression.
A tuning value can conceal the first bad change and teach the wrong architecture.

Known regression-shaped mechanisms in CDF:

- destination staged-window pressure incorrectly reducing global jobs;
- nested permits multiplying one CPU authority or serializing a frontier;
- canonical frontier drain waiting on partition order rather than assembling deterministically;
- unconditional remote Parquet range reads for high-coverage scans;
- another current-schema discovery pass before extraction;
- 1,024-row batch/segment proliferation;
- destination row-by-row materialization;
- entire-package materialization or re-read hashing;
- a retained handle/file descriptor per segment;
- transient free memory treated as a planning ceiling;
- full destination thread count used as scan/sink concurrency for very wide schemas.

These are hypotheses, not default explanations. Rank them from the first bad diff.

## Laptop versus promotion host

A laptop is appropriate for:

- reproduction;
- release-versus-debug mistakes;
- a deterministic bisect predicate;
- a fast falsification;
- control-flow profiling.

Laptop timing is invalidated by disk pressure, swap, thermal throttling, background builds, target
explosion, local public-network variance, and changed power mode. Inspect these cheaply before
believing a result.

Use the EC2 protocol to promote:

- a performance-affecting default;
- a cross-ticket baseline;
- a roofline/envelope claim;
- a large constant-memory acceptance;
- a regression floor intended for future tickets.

Read `.10x/skills/run-cdf-ec2-benchmarks/SKILL.md`.

## Default-change rule

A performance-affecting default may land only when:

- same-cell evidence preserves or improves the relevant path; or
- it fixes a correctness failure that would corrupt data or violate identity/commit invariants,
  followed by explicit measurement; or
- the new behavior is opt-in and the measured fastest safe default remains; or
- the ticket is cancelled/deferred with a no-action rationale.

Hard-coded low caps are not acceptable substitutes for admission. Prefer:

- explicit authoritative knobs;
- adaptive defaults resolved from compiled workload facts and admitted CPU/memory;
- plan-recorded resolutions for replay determinism;
- typed retry when a destination reports a real resource failure;
- observable telemetry that explains the choice.

More concurrency is not always faster. The default expands until CPU/device/destination saturation
or memory pressure, not to a magical constant.

## Performance changes must remove superseded paths

Once a faster, correct happy path is retained:

- delete old scalar/materialized/fallback code unless an active external compatibility requirement
  still owns it;
- remove tests that encode superseded CDF-internal behavior;
- keep one capability-shaped path in generic orchestration;
- preserve a slower compatibility fallback only when a real destination capability requires it and
  the sheet declares that boundary;
- delete experiment dependencies, features, build scripts, and tickets when falsified.

The nanoarrow/custom-DuckDB investigation is the canonical example: it identified the useful
parallel scanner mechanism, then CDF retained the stock in-process path and removed the packaging
complexity.

## Reporting conclusions honestly

State:

- what was measured;
- what changed;
- ratio and absolute result;
- whether correctness identities matched;
- whether memory/spill stayed within authority;
- confidence and environmental limitations;
- keep/kill/default/opt-in conclusion.

“Blazingly fast” is a product ambition. A benchmark conclusion is a bounded falsifiable statement.
