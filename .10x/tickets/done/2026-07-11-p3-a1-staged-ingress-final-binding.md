Status: done
Created: 2026-07-11
Updated: 2026-07-11
Parent: .10x/tickets/2026-07-10-p3-ws-a-streaming-runtime-pipeline.md
Depends-On: .10x/tickets/done/2026-07-10-p3-ws-l5-preoptimization-baseline.md, .10x/tickets/done/2026-07-11-p0-dx1-neutral-runtime-crate.md, .10x/specs/streaming-destination-ingress.md

# P3 A1: staged ingress and final package-binding contract

## Scope

Add neutral kernel/runtime types and state transitions for finalized-package-only and staged-ingress destination sessions, including `LoadAttemptId`, staged segment identity, resumable versus rollback/redrive recovery, final verified package binding, abort/reattach inspection, and compatibility adaptation for existing sessions. Do not yet overlap the engine pipeline or implement destination-specific staging.

## Acceptance criteria

- Staged acknowledgements cannot satisfy receipt/checkpoint APIs by type or serialization shape.
- Final binding requires a verified package hash/token and exact ordered manifest segment identities.
- Attempt identity is absent from deterministic plan/package/state preimages and jobs-invariance hashes.
- Existing finalized-only DuckDB/Postgres/Parquet behavior remains stable through compatibility adapters.
- Mock staged and finalized-only drivers pass mismatch, abort, crash-state, duplicate, and receipt-gate laws.

## Evidence expectations

Kernel/runtime tests, serialized artifact hash invariance, mock-driver conformance, crash-state matrix, dependency/layer checks, and adversarial architecture review.

## Explicit exclusions

No Tokio channels, destination staging implementation, bulk encoding, memory ledger, or throughput claim.

## Blockers

None. The pre-optimization baseline and DX1 neutral runtime extraction are complete.

## Progress and notes

- 2026-07-11: Added the neutral staged-ingress capability, attempt authority, durable-segment reader/request/acknowledgement, recovery snapshot, session, and verified final-binding contracts in `cdf-runtime`.
- 2026-07-11: Preserved finalized-package-only behavior as the default for DuckDB, Postgres, and Parquet; no destination-name branch entered the runtime contract.
- 2026-07-11: Adversarial review found that attempt reattachment initially bound only the opaque attempt id. Hardened the contract with an immutable `StagingAttemptBinding` covering destination, target, disposition, schema, and plan authority; recovery now rejects authority mismatch.
- 2026-07-11: Conformance proves staging acknowledgements cannot serialize package/receipt authority, exact ordered final identity matching, no receipt before binding, resumable reattachment, mismatched authority/segments, duplicate final package idempotency, repeated abort, and finalized-only failure.
- 2026-07-11: Evidence is recorded in `.10x/evidence/2026-07-11-p3-a1-staged-ingress-final-binding.md`; closure review passes in `.10x/reviews/2026-07-11-p3-a1-staged-ingress-final-binding-review.md`.

## References

- `.10x/decisions/destination-staged-ingress-final-package-binding.md`
- `.10x/decisions/destination-runtime-composition-boundary.md`
- `.10x/specs/destination-receipts-guarantees.md`
- `.10x/specs/package-lifecycle-determinism.md`
