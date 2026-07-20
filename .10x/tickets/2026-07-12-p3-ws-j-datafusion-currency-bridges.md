Status: open
Created: 2026-07-12
Updated: 2026-07-12
Parent: .10x/tickets/2026-07-10-p3-terabyte-scale-program.md
Depends-On: .10x/decisions/datafusion-analysis-scheduling-identity-boundary.md, .10x/specs/datafusion-currency-bridges.md

# P3 WS-J: DataFusion currency bridges

## Scope

Marshal CDF's existing statistics, memory/object-store authority, expression semantics, evidence stores, and native operator graph into DataFusion's standard interfaces without moving identity-bearing execution out of native CDF operators. This parent coordinates the bridge sequence and prevents P3 source/format/runtime work from hardening incompatible private representations.

## Activated children and existing owners

- `.10x/tickets/done/2026-07-12-p3-j0-typed-statistics-evidence-spine.md`
- `.10x/tickets/2026-07-12-p3-j1-evidence-statistics-pruning.md`
- `.10x/tickets/2026-07-12-p3-j2-datafusion-object-store-registry.md`
- `.10x/tickets/done/2026-07-11-p3-a2-unified-memory-ledger.md` — shared `MemoryPool` authority already complete
- `.10x/tickets/done/2026-07-12-p3-j3-expression-ir-contract-linter.md`
- `.10x/tickets/2026-07-12-p3-j4-evidence-catalog-adbc.md`
- `.10x/tickets/2026-07-12-p3-j5-execution-plan-marshaling-metrics.md`
- `.10x/tickets/2026-07-12-p3-j6-datafusion-selective-adoption-audit.md`
- `.10x/tickets/done/2026-07-11-p0-wx1-portable-partition-task-protocol.md` — canonical task authority reused by J5
- `.10x/tickets/done/2026-07-11-p0-fx1-native-format-extension-boundary.md` — neutral driver prerequisite for any optional DataFusion `FileFormat` adapter

## Sequencing

J0 is first because the readiness audit proved the typed per-column/per-segment evidence required by VISION is not implemented; it establishes the neutral identity-bearing currency. J1 then adapts that evidence without changing it. J2 follows the already-complete memory bridge and must land before DataFusion session use expands. J3 owns expression semantics before new declarative operators proliferate. J4 consumes J1 and the existing observability store. J5 follows WX1 and the streaming graph. J6 is an evidence-driven adoption audit and may create narrowly justified implementation owners; it cannot replace native primary paths by itself.

## Acceptance criteria

- DataFusion types remain confined to engine/adapter crates and never enter kernel or extension contracts.
- No DataFusion-generated data or verdict bytes enter package identity.
- Pruned and unpruned results are equivalent across the permanent adversarial matrix.
- DataFusion and native work share one memory and credential/object-store authority.
- Expression analysis is plan-recorded and replay-stable; native fused execution remains primary.
- Evidence catalogs are bounded, read-only, redacted, and ecosystem-queryable.
- Execution-plan shells expose unified metrics without changing task/package authority or jobs invariance.
- Existing P3/FX1/WX1 designs cite this boundary before their APIs become terminal.

## Evidence expectations

Static dependency checks, golden identity tests, pruning differential/property tests, memory/credential adversaries, expression differential fixtures, catalog large-history RSS, jobs/capsule invariance, pinned-tuple upgrade fixtures, and adversarial architecture review.

## Explicit exclusions

No primary-codec replacement, generic DataFusion package writer, kernel DataFusion types, implicit optimizer changes at replay, credential resolution inside DataFusion, or scheduler selection.

## Blockers

None at parent level. Child dependencies govern execution.

## Progress and notes

- 2026-07-12: Source audit found the original J1 sequence assumed evidence that VISION specifies but current execution does not emit. Added J0 and `.10x/specs/typed-statistics-evidence.md`; lexical `BatchStats`/aggregate `profile.json` must be replaced, not adapted or preserved.
