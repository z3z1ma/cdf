Status: open
Created: 2026-07-11
Updated: 2026-07-11
Parent: .10x/tickets/2026-07-11-p0-destination-extension-boundary.md
Depends-On: .10x/tickets/done/2026-07-11-p0-dx1-neutral-runtime-crate.md

# P0 DX2: driver-owned adapters and composition root

## Scope

Move DuckDB, Parquet, and Postgres runtime driver adapters into their destination crates; build one explicit CLI first-party registry; inject it into project entry points; remove builtin registration and production convenience constructors from shared runtime code.

## Acceptance criteria

- Destination-specific planning/private types remain inside destination crates.
- `cdf-project` removes all `cdf-dest-*` Cargo dependencies and imports.
- CLI composition is one auditable registration list; generic commands receive registry authority.
- Existing run/replay/resume/promotion artifacts and receipts remain stable.

## Blockers

None. DX1 is complete.

## Progress and notes

- 2026-07-11: Added driver-owned runtime adapters to DuckDB, Parquet, and Postgres crates against `cdf-runtime`, including typed no-mutation inspection, package-aware preparation, correction/replay behavior, secret/policy resolution, and explicit current bulk/ingress/concurrency declarations. The existing destination suites passed 21 DuckDB, 27 Parquet, and 40 Postgres tests. Project-side compatibility adapters remain active until the next tranche injects the CLI registry and removes concrete project dependencies.
- 2026-07-11: Deleted all project-owned runtime adapter modules and production convenience constructors. `cdf-cli/src/destination_registry.rs` is now the single first-party composition list, project resolution requires injected registry authority, and run/replay callers preserve prior redaction/not-supported behavior. The 273-test CLI suite reached 270 passes; its two registry-wording regressions and one pre-existing ledger-v5 expectation were repaired and all three focused reruns passed. DX3 still owns lock/doctor/replay residual branches before this ticket can close its full dependency criterion.
- 2026-07-11: Lock creation and schema pinning now receive driver-inspected `DestinationSheetArtifact` values; the project lock module's destination URI match tree and direct DuckDB/Parquet/Postgres sheet construction were deleted. DuckDB and Parquet moved out of the normal `cdf-project` build graph (remaining as test fixtures); the remaining normal Postgres edge is source catalog discovery owned by the source-extension boundary, while DX3 owns doctor/replay product residuals. Focused project and CLI contract/schema-lock tests passed.
- 2026-07-11: Generic driver declarations now own replay target requirements, target parsing, policy keys/allowed values, and health execution. CLI replay removed its Postgres URI/type branches; standard doctor and inspect destination paths consume one generic inspection/health view. DuckDB ICU behavior and typed JSON details remain driver-owned and focused doctor plus Postgres replay error-order/target/policy regressions pass. `doctor_drift.rs` remains the explicitly adapter-specific DuckDB mirror diagnostic allowed by the composition decision, not generic doctor orchestration.
