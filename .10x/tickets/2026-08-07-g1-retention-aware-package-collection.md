Status: open
Created: 2026-08-07
Updated: 2026-08-07
Parent: `.10x/tickets/2026-08-03-cdc-semantic-sql-project-foundation-program.md`

# G1: retention-aware package collection

## Scope

Implement the active retention-aware collection contract end to end: neutral policy resolution and
candidate planning, crash-safe/idempotent tombstone execution, dry-run/`--execute` CLI parity,
post-checkpoint automatic collection, promotion-availability reporting, telemetry, and focused
behavioral tests.

## Non-goals

- deleting package directories, receipts, checkpoints, or minimal settlement proof;
- remote archive/object-store retention;
- ledger/checkpoint history compaction;
- CDC adapter or keyed-effect implementation;
- a second CLI-owned eligibility algorithm.

## Acceptance criteria

- [ ] Trust/default retention resolves exactly to runs, duration, or disabled using committed
      checkpoint order/settlement time rather than filesystem time.
- [ ] Only verified receipt-and-checkpoint-settled packages outside retention become candidates;
      incomplete, leased, recovery-required, corrupt, ambiguous, and missing states fail closed.
- [ ] `cdf package gc` is dry-run and `cdf package gc --execute` revalidates and executes the same
      canonical plan with truthful human/JSON classifications and reclaimed counts.
- [ ] Normal execution invokes the same bounded collector after successful checkpoint settlement;
      collection failure cannot falsify the committed checkpoint and remains retryable/visible.
- [ ] Tombstoning is idempotent and preserves manifest/hash/receipt/checkpoint proof while replay
      availability and schema-promotion consequences are reported truthfully.
- [ ] Focused package/project/CLI behavioral tests and affected-package formatting/check/Clippy pass.

## References

- `.10x/specs/retention-aware-package-collection.md`
- `.10x/research/2026-08-07-cdc-mysql-continuous-readiness.md`
- `.10x/specs/checkpoint-state-commit-gate.md`
- `.10x/specs/schema-promotion-corrections.md`
- `.10x/specs/package-lifecycle-determinism.md`
- `.10x/knowledge/developer-build-duckdb-linkage.md`

## Assumptions

- The policy and lifecycle semantics are user-ratified and active in the governing spec.
- Current pre-production artifacts are updated directly; no compatibility reader or migration is
  permitted.

## Journal

- 2026-08-07: Ticket opened after user ratification and current-source readiness inspection. The
  existing CLI planner permanently protects receipted/checkpointed packages, while the package
  crate already supplies crash-safe tombstoning. Implementation will reuse one neutral planner for
  automatic and manual paths.

## Blockers

None. The ticket is executable after this record-publication turn.

## Evidence

Pending implementation.

## Review

Pending the one tranche-level adversarial review requested after the CDC tranche stabilizes.

## Retrospective

Pending implementation.
