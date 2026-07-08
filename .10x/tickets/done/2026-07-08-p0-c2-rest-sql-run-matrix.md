Status: done
Created: 2026-07-08
Updated: 2026-07-08
Parent: .10x/tickets/2026-07-07-p0-workstream-c-spine-conformance-harness.md
Depends-On: .10x/tickets/done/2026-07-08-p0-c1-run-spine-matrix-foundation.md

# P0 C2: REST and SQL run-spine matrix cells

## Scope

Extend the run-spine scenario matrix from C1 to deterministic REST-fixture and table-backed Postgres SQL source archetypes across DuckDB, filesystem Parquet, and Postgres destinations and append, replace, and merge where destination sheets support them.

Owns:

- splitting the C1 file-source test harness into focused modules before adding REST/SQL cells, so matrix expansion does not compound one large mixed test file;
- deterministic REST transport fixtures and runtime dependencies in conformance;
- deterministic Postgres SQL source fixtures using the existing local Postgres harness conventions;
- source-specific matrix fixture construction;
- matrix output evidence updates for REST and SQL executed/excluded cells.

## Acceptance Criteria

- REST cells use deterministic in-process/fake HTTP transport and do not contact the public network.
- The C1 run-matrix test harness is split into focused files/modules for matrix execution, file/REST/SQL fixtures, destination handles, assertion helpers, plan JSON construction, and local Postgres support before new REST/SQL cells are added.
- SQL cells use local ephemeral Postgres source setup consistent with existing destination/source tests.
- REST and SQL resources enter `run_project` through `ProjectRunSource::rest` and `ProjectRunSource::sql`, respectively.
- Each executed REST/SQL cell uses the same matrix assertions required by C1: plan honesty, package validity, trait-level receipt verification, checkpoint gating, replay identity, and duplicate behavior.
- Sheet-excluded cells are recorded with a reason in the matrix output, not skipped.
- Secret values used by REST/SQL fixtures are not written into evidence records or serialized artifacts except as redacted references.

## Evidence Expectations

Run focused REST/SQL matrix tests, relevant declarative runtime tests if helpers are touched, `cargo fmt --all --check`, `cargo check -p cdf-conformance -p cdf-project -p cdf-declarative --all-targets --locked`, `cargo clippy -p cdf-conformance -p cdf-project -p cdf-declarative --all-targets --locked -- -D warnings`, `cargo nextest run -p cdf-conformance --locked`, `git diff --check`, redaction-focused Gitleaks if fixture secrets are introduced, and updated matrix evidence with executed/excluded cells.

## Explicit Exclusions

No live GitHub/public HTTP API execution. No arbitrary SQL query source support beyond the already-ratified table-backed SQL resource path. No new destination/source archetype. No cross-destination chaos or property/fuzz work.

## Progress And Notes

- 2026-07-08: Split from P0 Workstream C. Existing project runtime tests already prove deterministic REST and table-backed SQL can run into DuckDB; this child moves that coverage into conformance and expands it across current MVP destinations/dispositions.
- 2026-07-08: C1 review found the first run-matrix test harness intentionally complete but already large at 1,137 lines. C2 must split that harness before extending it with REST/SQL cells.
- 2026-07-08: Activated for worker implementation. Worker owns the run-matrix harness split, deterministic REST and SQL source cells across current MVP destinations/dispositions, focused tests, and C2 progress notes. Parent owns review, final quality gates, evidence/review records, closure, commit, and push.
- 2026-07-08: Implemented the run-matrix split into focused modules for core execution, destination handles, FILE/REST/SQL fixtures, assertions, plan construction, local Postgres, and test transport/secrets. The matrix now covers FILE, REST, and SQL sources across DuckDB, filesystem Parquet, and Postgres destinations. Focused output records 24 executed cells and 3 sheet-backed exclusions: each source archetype has 8 executed cells and 1 Parquet merge exclusion. REST uses injected `RecordingTransport` and enters `run_project` through `ProjectRunSource::rest`; SQL uses local/TEST_DATABASE_URL Postgres with a secret-provider URL and enters through `ProjectRunSource::sql`. REST/SQL plans are planner-built, assert cursor checkpoint positions at `updated_at=20`, and share the C1 package/receipt/checkpoint/replay/duplicate assertion surface. Worker verification passed: `cargo fmt --all --check`, `cargo test -p cdf-conformance run_matrix -- --nocapture`, `cargo check -p cdf-conformance -p cdf-project -p cdf-declarative --all-targets --locked`, `cargo clippy -p cdf-conformance -p cdf-project -p cdf-declarative --all-targets --locked -- -D warnings`, `cargo nextest run -p cdf-conformance --locked`, `git diff --check`, and `gitleaks detect --no-git --source crates/cdf-conformance/src/run_matrix --report-format json --report-path target/quality/reports/gitleaks-p0-c2-run-matrix.json`.
- 2026-07-08: Parent verification reran the focused matrix, conformance nextest, fmt/check/clippy, Semgrep, Gitleaks, jscpd, rust-code-analysis, scc, cargo deny/audit/vet, cargo tree, and OSV scanner. Evidence is recorded in `.10x/evidence/2026-07-08-p0-c2-rest-sql-run-matrix.md`; adversarial review is recorded in `.10x/reviews/2026-07-08-p0-c2-rest-sql-run-matrix-review.md`. C2 is closed.

## Blockers

None. C1 is closed, the harness split is complete, and C2 is closed.
