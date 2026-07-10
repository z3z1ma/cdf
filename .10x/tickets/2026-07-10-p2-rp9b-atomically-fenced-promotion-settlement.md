Status: open
Created: 2026-07-10
Updated: 2026-07-10
Parent: .10x/tickets/2026-07-10-p2-rp9-promotion-execution-recovery-gc.md
Depends-On: .10x/tickets/done/2026-07-10-p2-rp9a-promotion-artifact-recovery-authority.md, .10x/tickets/done/2026-07-10-p2-rp4-schema-scope-lease-lock-cas.md

# RP9B atomically fenced promotion checkpoint and publication settlement

## Scope

Make promotion checkpoint and publication advancement conditional on the same current fencing token inside their atomic store mutations, while preserving idempotent destination settlement that may finish after lease expiry.

## Acceptance criteria

- Kernel/store traits represent fenced checkpoint commit and fenced promotion-publication append without filesystem or CLI semantics. Implementations can be provided by local SQLite now and future transactional stores later.
- SQLite checks the schema-contract lease owner/token/expiry inside the same transaction that commits a promotion checkpoint or inserts a publication event. A pre-check followed by an unfenced write is not sufficient.
- Destination settlement may finish after lease expiry only through the locked idempotent correction protocol; the expired executor stops before checkpoint, lock, or publication advancement, and a new owner recovers from the verified receipt/package.
- Exact lock CAS continues using RP4's integrated fence. Post-lock/pre-event recovery atomically verifies the current fence and appends one idempotent event.
- Post-lock recovery reconstructs and validates the complete correction request/plan/receipt contract, exact staged target set, committed checkpoints, and live destination verification before publication. Existing publication verification rejects missing, extra, or mismatched targets.
- Lease renewal is either implemented through the shared store abstraction or explicitly unnecessary because every long operation is followed by an atomically fenced advancement; no fixed-duration correctness assumption remains.
- Crash/expiry tests cover takeover during destination settlement, immediately before checkpoint commit, immediately before lock CAS, and immediately before publication insert.

## Evidence expectations

Kernel/store API tests, SQLite transaction/fencing tests, deterministic clocks/takeover races, exact target-set verification, post-lock repair, semver/migration compatibility, strict Clippy/formatting, and independent review.

## Explicit exclusions

No distributed 2PC, destination rollback after receipt, remote lease store, cross-destination command matrix, GC behavior, or Parquet policy.

## Progress and notes

- 2026-07-10: Opened from the TOCTOU finding in `.10x/reviews/2026-07-10-p2-rp9-promotion-execution-independent-review.md`. Destination settlement is intentionally allowed to complete after lease expiry; checkpoint/lock/publication authority is not.
- 2026-07-10: Read-only substrate preflight completed in `.10x/research/2026-07-10-rp9b-atomic-settlement-preflight.md`. The smallest sound boundary is one aggregate `PromotionSettlementStore: CheckpointStore + ScopeLeaseStore` over a single consistency domain, with separately atomic fenced checkpoint-commit and publication-append operations; exact filesystem lock CAS remains between them. Existing SQLite tables suffice without a data-schema migration. RP9B remains open and dependent on RP9A.

## Blockers

Depends on RP9A's exact persisted recovery authority.
