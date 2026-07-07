Status: active
Created: 2026-07-07
Updated: 2026-07-07
Parent: .10x/tickets/2026-07-05-implement-cdf-system.md
Depends-On: .10x/decisions/p0-structural-debt-stop-line.md

# P0 structural debt stop-the-line program

## Scope

Close the six structural-debt workstreams ratified by `.10x/decisions/p0-structural-debt-stop-line.md` before widening CDF into new enterprise destination, new source-archetype, CDC/Kafka, or streaming-supervisor lanes.

This parent is a plan and orchestration record. Child tickets own execution. The parent agent owns sequencing, workstream assignment, cross-record coherence, evidence review, coverage-matrix updates, and the final stop-line lift note.

## Child workstreams

- `.10x/tickets/2026-07-07-p0-workstream-a-streaming-commit-session.md`
- `.10x/tickets/2026-07-07-p0-workstream-b-open-orchestrator-world.md`
- `.10x/tickets/2026-07-07-p0-workstream-c-spine-conformance-harness.md`
- `.10x/tickets/2026-07-07-p0-workstream-d-dependency-tuple-residual.md`
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

## Blockers

None for the P0 parent. Child tickets own their own API decisions, implementation, evidence, and review.
