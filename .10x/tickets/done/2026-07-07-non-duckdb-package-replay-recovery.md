Status: done
Created: 2026-07-07
Updated: 2026-07-07
Parent: .10x/tickets/done/2026-07-07-general-run-orchestrator.md
Depends-On: .10x/specs/run-orchestration-ledger.md, .10x/specs/destination-receipts-guarantees.md, .10x/decisions/project-run-postgres-destination-inputs.md, .10x/tickets/done/2026-07-07-general-run-postgres-destination.md

# Add non-DuckDB package replay recovery

## Scope

Implement finalized-package/no-durable-receipt recovery for the non-DuckDB destinations already supported by the general project run runtime.

Owns:

- `crates/cdf-project/src/runtime.rs` public replay/recovery request shapes and helpers for filesystem Parquet and Postgres package artifacts.
- `crates/cdf-project/src/runtime_tests.rs` regression tests proving source-free replay/recovery after package finalization for Parquet and Postgres.
- Parent progress notes on `.10x/tickets/done/2026-07-07-general-run-orchestrator.md`.

## Acceptance criteria

- Parquet packages finalized by `run_project` can be replayed from package artifacts without a supplied receipt, without contacting the source, and finish by recording a destination receipt, committing the checkpoint, and updating package status.
- Postgres packages finalized by `run_project` can be replayed from package artifacts without a supplied receipt when the caller supplies the explicit Postgres destination inputs required by `.10x/decisions/project-run-postgres-destination-inputs.md`; replay MUST NOT infer target, dedup, existing-table policy, or merge semantics from destination introspection.
- Durable-receipt recovery paths for Parquet and Postgres remain available and continue to verify the receipt before checkpoint commit.
- Tests simulate post-finalization source loss or absence before replay/recovery, so the evidence proves no source contact after package finalization.
- Unsupported or under-specified Postgres artifact replay fails closed before destination or checkpoint mutation.

## Evidence expectations

Run focused `cdf-project` replay/recovery tests, destination tests needed by the touched surfaces, `cargo fmt`, clippy over touched crates, workspace check, and `git diff --check`. Record current-tree evidence and an adversarial review before closing this ticket.

## Explicit exclusions

No CLI `resume` or `replay package` wiring, no inspect-run output, no new destination introspection, no distributed recovery policy, no artifact format migration beyond what is required for this replay slice.

## Design notes

- DuckDB already has `replay_duckdb_package_from_artifacts`, so this ticket should mirror that shape where destination inputs are package-owned.
- Parquet replay can derive the needed commit inputs from package artifacts and destination configuration.
- Postgres replay needs explicit destination inputs because `PostgresLoadPlanInput` includes `PostgresTarget`, merge dedup policy, optional existing-table policy, and destination-owned column mapping. The package currently carries target/disposition/merge-key preimage data, but not all explicit Postgres destination policy required by the active decision.

## Blockers

None.

## Progress and notes

- 2026-07-07: Opened after parent closure audit found `.10x/tickets/done/2026-07-07-general-run-orchestrator.md` lacked finalized-package/no-receipt recovery evidence for Parquet and Postgres. Durable-receipt recovery was already tested, but the no-receipt package replay window was real missing runtime work.
- 2026-07-07: Implemented Parquet and Postgres package-artifact replay requests/functions, source-loss replay tests, and Postgres mismatched-target fail-closed coverage. Evidence recorded in `.10x/evidence/2026-07-07-non-duckdb-package-replay-recovery.md`; review recorded in `.10x/reviews/2026-07-07-non-duckdb-package-replay-recovery-review.md`.
