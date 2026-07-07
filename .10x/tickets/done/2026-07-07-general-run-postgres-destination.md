Status: done
Created: 2026-07-07
Updated: 2026-07-07
Parent: .10x/tickets/2026-07-07-general-run-orchestrator.md
Depends-On: .10x/specs/run-orchestration-ledger.md, .10x/specs/destination-receipts-guarantees.md, .10x/decisions/project-run-postgres-destination-inputs.md

# Add Postgres destination support to the general run orchestrator

## Scope

Extend `cdf-project` general run orchestration so a finalized package can be committed to Postgres through the package-aware Postgres load plan and `DestinationProtocol::begin` session path.

Owns:

- A `ProjectRunDestination` variant or equivalent request shape for Postgres.
- Construction of `PostgresLoadPlanInput` from ratified project/runtime inputs.
- Package replay, receipt verification, checkpoint commit, package status update, and run-ledger events for Postgres destination commits.
- Deterministic tests using the existing live local Postgres harness where available.

## Acceptance criteria

- A project run can commit a supported package to Postgres through `PostgresDestination::with_commit_request(...).begin(...)`.
- The run ledger records the same event order as the DuckDB and Parquet project-run paths.
- Receipt verification is destination-owned through `PostgresDestination::verify_receipt` before checkpoint commit.
- Unsupported Postgres combinations fail before source, package, destination, or checkpoint mutation.
- Crash recovery after a durable Postgres receipt commits the checkpoint and updates package status without contacting the source.

## Blockers

None from user. `.10x/decisions/project-run-postgres-destination-inputs.md` ratifies explicit destination/run configuration for `PostgresTarget`, existing-table policy, and merge dedup policy; package-schema-derived column mappings; descriptor-derived merge keys only when explicitly present; and no implicit destination-introspection semantics for this slice.

## Explicit exclusions

No CLI command parsing, no arbitrary SQL execution, no non-local credential discovery, no changes to Postgres destination receipt semantics.

## Evidence expectations

Run focused `cdf-project` tests, Postgres destination live tests when the harness is available, `cargo clippy -p cdf-project --all-targets --locked -- -D warnings`, and workspace check.

## Progress and notes

- 2026-07-07: Blocked during `.10x/tickets/2026-07-07-general-run-orchestrator.md` continuation. Inspection found `PostgresDestination::plan_load` and `DestinationProtocol::begin` are available, but project-level inputs for safe `PostgresLoadPlanInput` construction are not yet ratified.
- 2026-07-07: User ratified the first-slice Postgres input contract in `.10x/decisions/project-run-postgres-destination-inputs.md`, while clarifying that future destination introspection remains in scope for the overall product. This ticket is now executable as an explicit-input project-run slice.
- 2026-07-07: Implemented and verified Postgres project-run destination support. Evidence recorded in `.10x/evidence/2026-07-07-general-run-postgres-destination.md`. The implementation uses explicit Postgres run inputs, package-schema-derived columns after package finalization, preflight load-plan validation before mutation, destination-owned receipt verification before checkpoint commit, and durable-receipt recovery without source contact. Postgres project-run receipt source metadata is receipt-only because generic `CommitSession::finalize` does not return duplicate/no-op detail.
- 2026-07-07: Parent review recorded in `.10x/reviews/2026-07-07-general-run-postgres-destination-review.md`. The review found that an initial helper duplicated Postgres Arrow type mapping inside `cdf-project`; that was corrected by exposing `postgres_columns_for_schema` from `cdf-dest-postgres` and using the destination-owned mapping in the runtime.
