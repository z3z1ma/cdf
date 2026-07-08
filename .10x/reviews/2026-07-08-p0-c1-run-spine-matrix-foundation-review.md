Status: recorded
Created: 2026-07-08
Updated: 2026-07-08
Target: .10x/tickets/done/2026-07-08-p0-c1-run-spine-matrix-foundation.md
Verdict: pass

# P0 C1 run-spine matrix foundation review

## Target

Review of the C1 implementation in `crates/cdf-conformance/src/run_matrix/**`, `crates/cdf-conformance/src/lib.rs`, `crates/cdf-conformance/Cargo.toml`, `Cargo.lock`, and ticket graph updates.

## Findings

No blocking findings remain.

Resolved significant finding: the initial worker implementation could pass without Postgres coverage by excluding Postgres cells when `LivePostgres::start()` returned `None`. C1 requires Postgres coverage. The final harness now requires a live Postgres path through `TEST_DATABASE_URL` or local `initdb`/`pg_ctl`, fails loudly on setup/schema errors, and asserts that the Postgres merge cell executes.

Resolved significant finding: schema creation errors were originally downgraded through `.ok()?`. The final `LivePostgres::start()` returns `Result<Self>` and propagates connection/schema/setup failures.

Minor residual with durable owner: `crates/cdf-conformance/src/run_matrix/tests.rs` is 1,137 lines and currently owns matrix execution, fixture construction, destination handles, assertion helpers, plan JSON, and local Postgres support. Individual functions remain modest in cognitive complexity, and the scope is acceptable for C1 because it establishes the first full matrix slice. C2 now explicitly requires splitting this harness before adding REST/SQL cells.

## Acceptance mapping

- Matrix cells are represented as `(source_archetype, destination, disposition)` through `RunMatrixCell`.
- Unsupported cells are recorded as `ExcludedMatrixCell`; the only C1 exclusion is Parquet merge, backed by the Parquet destination sheet.
- FILE-source cells execute through `cdf_project::run_project`, not destination-specific wrappers.
- DuckDB, filesystem Parquet, and Postgres are covered.
- Append and replace execute where supported; merge executes for DuckDB/Postgres and is sheet-excluded for Parquet.
- Executed cells assert plan honesty, package verification, trait-level destination receipt verification, checkpoint gating, artifact replay identity, and duplicate no-op behavior.
- Matrix output is captured in `.10x/evidence/2026-07-08-p0-c1-run-spine-matrix-foundation.md`.

## Verdict

Pass. C1 is supported by focused test output, full conformance nextest, clippy/check/fmt, duplication and complexity metrics, Semgrep, Gitleaks, and supply-chain checks. The remaining harness-size concern is not a C1 closure blocker because it is now an explicit C2 prerequisite before extension.
