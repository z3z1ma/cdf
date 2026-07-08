Status: active
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
- `.10x/tickets/2026-07-07-p0-workstream-c-spine-conformance-harness.md`
- `.10x/tickets/done/2026-07-07-p0-workstream-d-dependency-tuple-residual.md`
- `.10x/tickets/2026-07-07-p0-workstream-e-contract-depth-program.md`
- `.10x/tickets/2026-07-07-p0-workstream-f-benchmark-gate.md`

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

## Blockers

None for the P0 parent. Child tickets own their own API decisions, implementation, evidence, and review.
