Status: blocked
Created: 2026-07-07
Updated: 2026-07-07
Parent: .10x/tickets/2026-07-07-general-run-orchestrator.md
Depends-On: .10x/specs/run-orchestration-ledger.md, .10x/specs/destination-receipts-guarantees.md

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

- Current project-run inputs do not carry enough Postgres-specific semantic data to build `PostgresLoadPlanInput` without guessing: `PostgresTarget`, `PostgresColumn` mappings, merge keys as `PostgresIdentifier`s, merge dedup policy, and existing-table policy.
- The current ticket has not ratified whether those values should be derived from resource descriptors, package schema, project environment destination URI, destination introspection, or explicit run request fields.

## Explicit exclusions

No CLI command parsing, no arbitrary SQL execution, no non-local credential discovery, no changes to Postgres destination receipt semantics.

## Evidence expectations

Run focused `cdf-project` tests, Postgres destination live tests when the harness is available, `cargo clippy -p cdf-project --all-targets --locked -- -D warnings`, and workspace check.

## Progress and notes

- 2026-07-07: Blocked during `.10x/tickets/2026-07-07-general-run-orchestrator.md` continuation. Inspection found `PostgresDestination::plan_load` and `DestinationProtocol::begin` are available, but project-level inputs for safe `PostgresLoadPlanInput` construction are not yet ratified.
