Status: active
Created: 2026-07-11
Updated: 2026-07-14
Parent: .10x/tickets/2026-07-10-p3-ws-c-deterministic-parallelism.md
Depends-On: .10x/tickets/done/2026-07-11-p3-c3-engine-ffi-parallel-integration.md

# P3 C4: jobs-invariance and scaling matrix

## Scope

Make jobs 1/N invariance permanent across source/format/destination archetypes, run scaling to each roofline under skew/failure/limit/rate/scope constraints, and close local parallelism triage with measured evidence.

## Acceptance criteria

- Every permanent archetype produces identical semantic artifacts/receipt identities at jobs 1/2/auto/N.
- Scaling continues until a named hardware/network/destination limit; scheduler overhead and speculative waste are bounded.
- Oversubscription, starvation, slow-frontier, scope conflicts, and single-writer cases remain green.
- The local-partition-parallelism triage closes into evidence/no-action items.

## Evidence expectations

Generated invariance matrix/hashes, host scaling curves/profiles, stress/chaos output, triage reconciliation, and adversarial skew review.

## Explicit exclusions

No distributed scheduler.

## Blockers

None. C1–C3 are complete.

## Assumptions

- Record-backed: jobs may change scheduling, queue timing, and runtime metrics, but package identity, canonical segments, rows, positions, verdicts, quarantine, lineage, state preimages, and receipt package/segment identities must remain equal to jobs=1.
- Record-backed: one benchmark/run must use one shared `ExecutionServices` authority across transport, format, engine, and destination. A harness that constructs separate hosts cannot support scaling or oversubscription claims.
- Record-backed: C4 owns the permanent invariance matrix and measured scaling boundaries. It does not duplicate format/destination roofline optimization owned by B/D/G/V; a named downstream bottleneck becomes evidence and owner input, not an in-ticket rewrite.

## Journal

- 2026-07-14 activation: C3 closed with a fresh pass and the active backlog fell to 83 tickets. C4 is selected over continuing a single format thread because it is the immediate dependency for C5 isolated-worker equivalence, F4 one-terabyte closure, G4 TLC remote-I/O closure, and V3 validation-envelope closure.
- 2026-07-14 harness audit: the current prepared-file benchmark constructs `ExecutionServices` privately inside `benchmark_file_dependencies()` while engine execution falls back to a second default host and exposes no jobs dimension. This makes its scaling measurements structurally invalid. First implementation step is one injected run authority plus jobs 1/2/auto/N and canonical artifact fingerprints; the existing run matrix supplies source/destination coverage but its two-row, usually single-partition fixtures cannot by themselves prove parallel scaling.
- 2026-07-14 shared-authority repair: the first jobs-aware worker run exposed a second false path: the benchmark resolved a concrete file resource but discarded the compiler-owned `CompiledSourcePlan`, so the engine correctly rejected execution. The prepared file worker now follows the generic source lifecycle (compile, schema-authority bind, registry resolve, engine bind, operator-graph bind) and shares one run-scoped `ExecutionServices` authority through discovery, transport, codec, scheduler, and engine. The worker now records configured/effective jobs, limiting factors, package hash, and canonical segment entries. `CARGO_BUILD_JOBS=12 cargo check -p cdf-benchmarks --all-targets --locked` and the isolated prepared-worker phase test pass. This repairs the measurement boundary; it does not yet prove the jobs-invariance acceptance criterion.
- 2026-07-14 first permanent matrix cell: replaced the prepared worker's one-file-only request with `source_root + glob`, made physical-byte accounting derive from all compiler-planned partitions, and added a four-file NDJSON law at jobs 1/2/auto/4. The run records four effective workers for auto/4 on this host and produces identical package hashes and segment entries in all four modes while processing 8,192 rows. `CARGO_BUILD_JOBS=12 cargo test -p cdf-benchmarks --test lab_runners --locked` passed all 10 lab-runner tests. This establishes the local multi-file package cell; other format/destination/failure/skew/scaling cells remain open.
- 2026-07-14 native format expansion: the same four-partition jobs 1/2/auto/4 law now executes CSV, JSON document, NDJSON, and Parquet through their registered drivers. Every format reaches effective jobs 4 for auto/N and retains byte-identical package hashes and segment entries across job counts. The focused 16-run matrix passed in 3.59 seconds; destination, skew/failure, and measured roofline cells remain open.
- 2026-07-14 destination ingress expansion: added a project-run matrix using the ordinary receipt/checkpoint gate, not package-only simulation. Four Parquet source partitions execute at jobs 1 and 4 into both staged-segment DuckDB and finalized-package Parquet ingress. Each reaches the requested effective jobs and preserves package hash, receipt package hash, receipt segment ids, state segment ids, and row count. The focused staged/finalized test passed in 1.96 seconds. PostgreSQL remains joined through its accepted D3/D5 binary-COPY evidence rather than requiring an ambient database in the fast matrix; skew/failure and scaling evidence remain open.
- 2026-07-14 large-run scaling: measured a fixed 8,590,037,948-byte, four-partition FineWeb Parquet input through the complete receipt/checkpoint path into filesystem Parquet. Wall time improved 52.86s -> 43.32s -> 40.67s for jobs 1/2/4; peak RSS remained 0.665/1.000/1.534 GB under the 4 GiB budget. Jobs=4 package execution was 7.329s and finalized destination write/receipt was 33.069s, naming the destination as the scaling knee rather than hiding a flat curve. Evidence: `.10x/evidence/2026-07-14-p3-c4-fineweb-local-scaling.md`.

## References

- `.10x/tickets/done/2026-07-07-local-partition-parallelism-triage.md`
- `.10x/specs/deterministic-parallel-scheduler.md`
- `.10x/evidence/2026-07-14-p3-c4-fineweb-local-scaling.md`
