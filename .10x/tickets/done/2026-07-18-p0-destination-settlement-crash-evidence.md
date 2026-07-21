Status: done
Created: 2026-07-18
Updated: 2026-07-18
Parent: .10x/tickets/done/2026-07-18-p0-post-iceberg-integration-stabilization.md

# P0: close destination settlement crash evidence

## Scope

Prove recovery when a destination has durably committed and its receipt verifies but package-local receipt persistence has not occurred, and preserve any checkpoint-abandonment failure alongside the primary failure.

## Non-goals

- Changing destination idempotency, receipt shapes, or checkpoint-gate ordering.
- Adding destination-specific settlement branches.
- Treating the run ledger as settlement authority.

## Acceptance Criteria

- A failpoint stops replay after destination receipt verification and before package receipt persistence.
- The interrupted package has no receipt, its checkpoint remains proposed, and idempotent artifact replay settles the package without duplicating destination rows.
- Every pre-settlement checkpoint-abandon path reports abandonment failure alongside the primary failure.
- Project lifecycle tests and workspace static checks pass.

## References

- `.10x/specs/checkpoint-state-commit-gate.md`
- `.10x/specs/package-lifecycle-determinism.md`
- `.10x/specs/run-orchestration-ledger.md`
- `.10x/specs/streaming-destination-ingress.md`

## Assumptions

- Record-backed: package artifacts and destination idempotency tokens are sufficient to re-drive a verified destination commit when the package-local receipt append did not happen.
- Record-backed: checkpoint abandonment is cleanup and its failure must not replace or disappear behind the primary error.

## Journal

- 2026-07-18: Activated from the independent full-tranche audit. Existing receipt failpoints run after package receipt persistence and therefore do not exercise the destination-committed/package-unsettled boundary; ten abandonment calls currently discard cleanup errors.
- 2026-07-18: Added an exact pre-package-receipt failpoint after destination receipt verification. All post-proposal/pre-settlement failures now route through one abandonment helper that preserves cleanup failure alongside the primary error; the ten discarded abandonment results are gone. Artifact replay re-drives the same idempotency token and settles the proposed checkpoint without duplicating rows.
- 2026-07-18: The exact workspace barrier completed with 1,771/1,771 tests green, including the new settlement test and the existing crash/recovery matrix. Strict workspace all-target Clippy passed with warnings denied.

## Blockers

None.

## Evidence

- `verified_destination_receipt_before_package_record_replays_idempotently` passed: the injected crash leaves no package receipt and a proposed checkpoint, then artifact replay commits once and settles both authorities.
- `checkpoint_abandonment_failure_is_attached_to_primary_replay_failure` passed and proves the cleanup failure remains visible beside the primary replay error.
- `CARGO_BUILD_JOBS=12 DUCKDB_DOWNLOAD_LIB=1 cargo nextest run --workspace --locked -j 12 --no-fail-fast` ran 1,771 tests: 1,771 passed, 40 explicitly skipped.
- `CARGO_BUILD_JOBS=12 DUCKDB_DOWNLOAD_LIB=1 cargo clippy --workspace --all-targets --locked -j 12 -- -D warnings` passed.

## Review

Verdict: pass. A fresh-hat sequential review traced proposal, destination mutation, independent receipt verification, package receipt persistence, checkpoint commit, and every `abandon` call in replay. The new failpoint is exactly after verified durable destination settlement and before package receipt persistence; operational failures before settlement preserve abandonment errors, while deliberate crash hooks retain the proposed checkpoint for recovery.

Residual risk: none within scope. The collaboration thread limit prevented commissioning a new independent agent without reusing an old reviewer, so this is explicitly a sequential self-review rather than independent review.

## Retrospective

The existing “after receipt” hooks were named too coarsely and all occurred after package persistence, leaving the most important settlement window untested. Failpoints must name exact authority boundaries. Cleanup errors also need one shared combiner; repeated `let _ = abandon(...)` patterns made evidence loss easy.
