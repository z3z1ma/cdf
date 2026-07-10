Status: open
Created: 2026-07-10
Updated: 2026-07-10
Parent: .10x/tickets/2026-07-10-p2-residual-schema-promotion-program.md
Depends-On: .10x/tickets/done/2026-07-10-p2-rp4-schema-scope-lease-lock-cas.md, .10x/tickets/2026-07-10-p2-rp5-schema-promote-dry-planner-cli.md, .10x/tickets/done/2026-07-10-p2-rp6-postgres-in-place-corrections.md, .10x/tickets/done/2026-07-10-p2-rp7-duckdb-in-place-corrections.md, .10x/tickets/2026-07-10-p2-rp8-parquet-correction-sidecars.md

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

## Blockers

Depends on RP4-RP8.
