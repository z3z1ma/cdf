Status: active
Created: 2026-07-10
Updated: 2026-07-10
Parent: .10x/tickets/2026-07-10-p2-residual-schema-promotion-program.md
Depends-On: .10x/tickets/done/2026-07-10-p2-rp4-schema-scope-lease-lock-cas.md, .10x/tickets/done/2026-07-10-p2-rp5-schema-promote-dry-planner-cli.md, .10x/tickets/done/2026-07-10-p2-rp6-postgres-in-place-corrections.md, .10x/tickets/done/2026-07-10-p2-rp7-duckdb-in-place-corrections.md, .10x/tickets/done/2026-07-10-p2-rp8-parquet-correction-sidecars.md

# P2 RP9 promotion execution, recovery, and GC availability

## Scope

Add `cdf schema promote ... --execute`, composing lease acquisition, staged snapshot/plan artifacts, correction packages, destination settlement, promotion checkpoints, atomic lock publication, resume/recovery, and retention availability reporting across the implemented destination strategies.

## Acceptance criteria

- Execution follows the exact six-step order in `.10x/specs/schema-promotion-corrections.md`.
- Each correction is an immutable package with old/new schema, source residual package, original row addresses, strategy, validation program, receipt, and checkpoint evidence.
- Lock publication occurs only after all required target checkpoints and uses lease fencing plus exact CAS.
- Every crash boundary resumes from verified artifacts/receipts without blind rewrites or duplicate state, including idempotent publication-event repair after a successful lock CAS.
- Concurrent run/pin/promotion conflicts are named and cannot overwrite schema authority.
- `cdf package gc` reports last locally promotable residual bytes; promotion distinguishes retained package, verified destination readback, tombstone-only, and missing.
- No retention class silently becomes indefinite and no destination readback is inferred.
- P1 rendering and JSON output provide current phase, committed targets, remaining action, and exact recovery command.

## Evidence expectations

Cross-destination execution, lease/CAS contention, full promotion crash matrix, package/receipt/checkpoint/lock inspection, GC before/after cases, replay determinism, secret redaction, and adversarial review.

## Explicit exclusions

No distributed scheduler, remote lease store, automatic promotion, arbitrary update SQL, or source re-extraction implementation.

## Progress and notes

- 2026-07-10: Opened as the integration owner after destination strategies and dry planning.
- 2026-07-10: Activated after RP4-RP8 closed and assigned to `/root/impl_i5`. Execution must consume the exact typed version-3 artifact and package/receipt/target graph emitted by RP5; it may not reconstruct, flatten, or reinterpret dry-plan authority. The integration must stay capability-driven across destinations and route every correction through immutable packages, canonical destination sessions/receipts, schema-contract checkpoints, fenced lease authority, exact lock CAS, and the existing ledger abstraction. GC reporting is evidence classification only and must not invent retention or readback authority.
- 2026-07-10: Implemented the shared six-step execution orchestrator, `schema promote --execute`, immutable correction packages, append-only idempotent publication records, exact staged-plan resume, all six persisted crash branches, P1/JSON execution rendering, generated CLI artifacts, and local promotable-residual GC reporting. DuckDB addressed correction executes end to end and idempotent replay is proven. Evidence: `.10x/evidence/2026-07-10-p2-rp9-promotion-execution-recovery-gc.md`.
- 2026-07-10: Adversarial cross-destination probing found that Parquet sidecar settlement is implemented but promotion planning correctly rejects the sheet's `object-key-component-v1` as a column identifier policy. C3 explicitly deferred that semantic choice, so no adapter was invented. Owner: `.10x/tickets/done/2026-07-10-parquet-promotion-identifier-policy.md`. Review: `.10x/reviews/2026-07-10-p2-rp9-promotion-execution-recovery-gc-review.md`.
- 2026-07-10: Independent review failed the implementation candidate at `.10x/reviews/2026-07-10-p2-rp9-promotion-execution-independent-review.md`. The exact lock-CAS skeleton, capability dispatch, append-only publication store, and destination-readback restraint remain useful, but package-only recovery, staged identity, source receipt authority, custom-contract checkpoint scope, atomic checkpoint/publication fencing, structured recovery output, cross-destination/concurrency proof, and GC promotability require repair.
- 2026-07-10: RP9D closed as `.10x/tickets/done/2026-07-10-p2-rp9d-gc-promotion-availability.md` after independent pass review. GC now delegates to shared authenticated promotion-read availability without changing retention or inferring destination readback. The Parquet namespace/capability ticket also closed; RP9A repair, RP9B fencing, and RP9C integration remain.

## Child repair sequence

- RP9A owns self-authenticating staged/correction artifacts and source-free recovery after packaging.
- RP9B depends on RP9A and owns atomically fenced checkpoint/publication settlement.
- The Parquet identifier-policy ticket proceeds independently under its ratified namespace decision.
- RP9D owns truthful GC promotion availability independently of RP9A/RP9B.
- RP9C integrates RP9A/RP9B/Parquet into multi-target, cross-destination, concurrency, migration, secret, and P1/JSON command conformance.

## Blockers

RP9 remains active until RP9A-RP9C close with integrated evidence and a new independent pass review.
