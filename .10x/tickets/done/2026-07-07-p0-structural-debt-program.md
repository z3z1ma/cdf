Status: done
Created: 2026-07-07
Updated: 2026-07-08
Parent: .10x/tickets/2026-07-05-implement-cdf-system.md
Depends-On: .10x/decisions/p0-structural-debt-stop-line.md

# P0 structural debt stop-the-line program

## Scope

Close the six structural-debt workstreams ratified by `.10x/decisions/p0-structural-debt-stop-line.md` before widening CDF into new enterprise destination, new source-archetype, CDC/Kafka, or streaming-supervisor lanes.

This parent is a plan and orchestration record. Child tickets own execution. The parent agent owns sequencing, workstream assignment, cross-record coherence, evidence review, coverage-matrix updates, and the final stop-line lift note.

## Child workstreams

- `.10x/tickets/done/2026-07-07-p0-workstream-a-streaming-commit-session.md`
- `.10x/tickets/done/2026-07-07-p0-workstream-b-open-orchestrator-world.md`
- `.10x/tickets/done/2026-07-07-p0-workstream-c-spine-conformance-harness.md`
- `.10x/tickets/done/2026-07-07-p0-workstream-d-dependency-tuple-residual.md`
- `.10x/tickets/done/2026-07-07-p0-workstream-e-contract-depth-program.md`
- `.10x/tickets/done/2026-07-07-p0-workstream-f-benchmark-gate.md`

## Stop-line rule

Until Workstreams A, B, and C close, do not open new:

- BigQuery, Snowflake, Databricks, Iceberg, Delta, or other destination lanes;
- CDC, Kafka, or other new source-archetype lanes;
- resident streaming-supervisor implementation lanes.

Work already in flight may finish its current child ticket, then pauses. Workstreams D, E, and F may run concurrently with A-C in conflict-free lanes.

## Acceptance criteria

- Workstreams A-F are done with evidence and adversarial review records.
- A-C close before the stop-line is lifted for new destination, source-archetype, and streaming-supervisor lanes.
- Destination/session specs reflect the required segment-streaming API and trait-level receipt verification.
- The coverage matrix references this P0 program and its relevant workstream owners.
- Destination/source parent tickets reflect the stop-line where they could otherwise be picked up incorrectly.
- Final parent progress explicitly states that the stop-line is lifted, or remains active with the remaining blocker named.

## Evidence expectations

Record a parent evidence rollup with:

- the six child evidence records;
- A-C close status and stop-line lift status;
- before/after public API and module-shape notes for kernel/project/runtime surfaces;
- conformance, golden, chaos, property/fuzz, dependency, supply-chain, benchmark, jscpd, and rust-code-analysis evidence where applicable.

## Explicit exclusions

No implementation is authorized by this parent directly. No new warehouse/lakehouse destination, CDC/Kafka source archetype, resident streaming supervisor, distributed scheduler, WASM host, registry, or public performance claim may be opened under this parent unless the relevant child ticket explicitly owns it and the stop-line permits it.

## Progress and notes

- 2026-07-07: Opened from user-ratified P0 structural-debt directive. Workstreams A-C are the stop-line gate. D-F may proceed concurrently in conflict-free lanes.
- 2026-07-07: Activated Workstream A and ratified `.10x/decisions/commit-session-segment-write-api.md` so implementation workers have a stable kernel session contract.
- 2026-07-07: Closed Workstream D at `.10x/tickets/done/2026-07-07-p0-workstream-d-dependency-tuple-residual.md` with dependency-tree evidence, DuckDB Arrow 58 private-driver residual decision, DataFusion crates.io Arrow 59 tripwire, supply-chain gate output, and adversarial review. DataFusion remains mandatory; the git pin remains time-boxed and publication-blocking until the Arrow 59 crates.io tuple exists.
- 2026-07-07: Closed Workstream A at `.10x/tickets/done/2026-07-07-p0-workstream-a-streaming-commit-session.md` with the segment-write kernel API, required `DestinationProtocol::begin`, trait-level receipt verification, DuckDB/Parquet/Postgres segment sessions, runtime segment feeding, focused quality evidence in `.10x/evidence/2026-07-07-streaming-commit-session-api.md`, and adversarial review in `.10x/reviews/2026-07-07-streaming-commit-session-api-review.md`.
- 2026-07-07: Stop-line remains active. Workstreams B and C are still open, so new warehouse/lakehouse destination lanes, new CDC/Kafka/source-archetype lanes, and resident streaming-supervisor implementation lanes remain paused.
- 2026-07-07: Shaped Workstream B with read-only inventory in `.10x/research/2026-07-07-open-orchestrator-world-inventory.md`, API decision `.10x/decisions/project-destination-driver-registry.md`, and four child tickets for runtime foundation, generic replay/recovery, generic project run/resolution, and caller migration/wrapper deletion.
- 2026-07-08: Closed Workstream B at `.10x/tickets/done/2026-07-07-p0-workstream-b-open-orchestrator-world.md` with generic project destination registration/resolution, generic run/replay/recovery paths, generic stage/failpoint seams, caller migration, wrapper deletion, B4 quality evidence, and aggregate review.
- 2026-07-08: Stop-line remains active. Workstream C is still open, so new warehouse/lakehouse destination lanes, new CDC/Kafka/source-archetype lanes, and resident streaming-supervisor implementation lanes remain paused.
- 2026-07-08: Split Workstream C into child tickets for run matrix foundation, REST/SQL matrix expansion, cross-destination chaos, per-destination live-run goldens, property/fuzz targets, and closure rollup.
- 2026-07-08: Closed Workstream C child C1 at `.10x/tickets/done/2026-07-08-p0-c1-run-spine-matrix-foundation.md`. The A-C stop-line remains active because Workstream C children C2-C6 remain open.
- 2026-07-08: Closed Workstream C child C2 at `.10x/tickets/done/2026-07-08-p0-c2-rest-sql-run-matrix.md`. The A-C stop-line remains active because Workstream C children C3-C6 remain open.
- 2026-07-08: Closed Workstream C child C3 at `.10x/tickets/done/2026-07-08-p0-c3-cross-destination-chaos.md`. The A-C stop-line remains active because Workstream C children C4-C6 remain open.
- 2026-07-08: Closed Workstream C child C4 at `.10x/tickets/done/2026-07-08-p0-c4-live-run-goldens-per-destination.md`. The A-C stop-line remains active because Workstream C children C5-C6 remain open.
- 2026-07-08: Closed Workstream C child C5 at `.10x/tickets/done/2026-07-08-p0-c5-property-fuzz-targets.md`. The A-C stop-line remains active because Workstream C child C6 aggregate closure remains open.
- 2026-07-08: Closed Workstream C at `.10x/tickets/done/2026-07-07-p0-workstream-c-spine-conformance-harness.md`. Workstreams A, B, and C are now closed, so the A-C stop-line is lifted for new destination lanes, new source-archetype lanes, and resident streaming-supervisor implementation lanes. The broader P0 program remains active because Workstreams E and F remain open.
- 2026-07-08: Activated Workstream E graph. P1 contract-depth parent `.10x/tickets/done/2026-07-08-p1-contract-depth-program.md`, ordered children E1-E6, and decision `.10x/decisions/contract-live-verdict-execution-semantics.md` are open; implementation has not started yet.
- 2026-07-08: Closed Workstream F at `.10x/tickets/done/2026-07-07-p0-workstream-f-benchmark-gate.md` with private `crates/cdf-benchmarks`, deterministic fixture specs, smoke/full/postgres Criterion suites, JSONL trend recording, scoped cargo-vet exemptions for benchmark-only dependencies, baseline evidence in `.10x/evidence/2026-07-08-p0-workstream-f-benchmark-gate.md`, and review in `.10x/reviews/2026-07-08-p0-workstream-f-benchmark-gate-review.md`. The broader P0 program remains active because Workstream E is not closed.
- 2026-07-08: Closed P1 E5 at `.10x/tickets/done/2026-07-08-p1-e5-trust-ring-ledger-events.md` with explicit anomaly-fact demotion semantics ratified by `.10x/decisions/contract-anomaly-signal-demotion-policy.md`, closure evidence in `.10x/evidence/2026-07-08-p1-e5-trust-ledger-events.md`, and review in `.10x/reviews/2026-07-08-p1-e5-trust-ledger-events-review.md`. The broader P0 program remains active because Workstream E still requires E6 drift-quarantine conformance.
- 2026-07-08: E6 is partially implemented but blocked on literal source scalar type-drift quarantine. The missing lower-layer seam is ratified by `.10x/decisions/source-decode-type-drift-quarantine.md` and owned by `.10x/tickets/done/2026-07-08-source-decode-type-drift-quarantine-seam.md`; P0 remains active.
- 2026-07-08: Closed Workstream E at `.10x/tickets/done/2026-07-07-p0-workstream-e-contract-depth-program.md` after P1 E6 and `.10x/tickets/done/2026-07-08-source-decode-type-drift-quarantine-seam.md` completed. All six P0 workstreams are now closed with evidence and adversarial review. The P0 structural-debt directive is exited and the stop-line is lifted in full; `.10x/knowledge/runtime-conformance-throughput-rule.md` remains permanently in force. Exit evidence: `.10x/evidence/2026-07-08-p0-structural-debt-program-exit.md`; review: `.10x/reviews/2026-07-08-p0-structural-debt-program-exit-review.md`.

## Blockers

None.
