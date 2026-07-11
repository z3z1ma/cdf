Status: open
Created: 2026-07-11
Updated: 2026-07-11
Parent: .10x/tickets/2026-07-10-p3-ws-a-streaming-runtime-pipeline.md
Depends-On: .10x/tickets/done/2026-07-10-p3-ws-l5-preoptimization-baseline.md, .10x/tickets/done/2026-07-11-p0-dx1-neutral-runtime-crate.md, .10x/specs/runtime-memory-backpressure.md

# P3 A2: unified memory ledger and accounted payloads

## Scope

Create the lightweight `cdf-memory` contract crate and implement the default shared DataFusion-backed coordinator adapter, budget resolution/headroom evidence, named reservations/telemetry, accounted batch/byte envelopes, async availability/admission, source poll working-set bridge, and discovery weighted-permit migration. Establish APIs and conformance before converting every spillable operator.

## Acceptance criteria

- DataFusion and CDF buffers reserve from the same finite pool.
- Accounted payload clones/allocations and all drop/error/cancel paths reconcile exactly in focused tests.
- Minimum-working-set admission prevents cyclic hold-and-wait in adversarial stage tests.
- Discovery genuinely enforces per-file/total/concurrency limits and uses parallel slots only after weighted permits.
- Effective process/headroom/managed/spill budgets and peak consumer facts are reportable.
- A mock external source/destination consumes the neutral accounting API without DataFusion/project/CLI dependencies, and static dependency tests preserve that boundary.
- Resource conformance falsifies understated maximum poll/decode working-set declarations.

## Evidence expectations

Memory-pool/reservation tests, Loom or equivalent concurrency model tests where practical, discovery scheduler tests, RSS calibration linkage, artifact/hash invariance, dependency graph checks, and adversarial review.

## Explicit exclusions

No full Tokio operator pipeline, production dedup spill, adaptive batch controller, destination bulk implementation, or 100 GB completion claim.

## Blockers

None. L5 baseline/headroom evidence and DX1 are complete.

## Progress and notes

- 2026-07-11: Created the implementation-neutral `cdf-memory` crate with typed consumers/classes/sub-caps, shared RAII leases, accounted Arrow/byte envelopes, weighted async admission, deterministic coordinator, JSON-reportable current/peak/wait/flush/spill telemetry, and process/headroom/managed budget resolution.
- 2026-07-11: Focused conformance covers clone/drop/error reconciliation, weighted sub-cap wakeup, oversized minimum-working-set rejection, budget safety, report serialization, and a static no-DataFusion/Tokio/project/engine/destination/runtime dependency law.
- 2026-07-11: Ratified calibrated `native-headroom-v1` in `.10x/decisions/runtime-native-headroom-policy-v1.md`. The ticket remains active for the DataFusion adapter, source working-set bridge, discovery migration, and external extension conformance.

## References

- `.10x/decisions/runtime-memory-ledger-byte-permits.md`
- `.10x/research/2026-07-11-runtime-memory-accounting-audit.md`
- `.10x/specs/architecture-layering-runtime.md`
- `.10x/specs/data-onramp-schema-intelligence.md`
