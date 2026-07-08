Status: done
Created: 2026-07-08
Updated: 2026-07-08
Parent: .10x/tickets/done/2026-07-07-p0-workstream-c-spine-conformance-harness.md
Depends-On: .10x/tickets/done/2026-07-07-p0-workstream-a-streaming-commit-session.md, .10x/tickets/done/2026-07-07-p0-workstream-b-open-orchestrator-world.md

# P0 C1: Run-spine matrix foundation

## Scope

Create the conformance-owned run-spine scenario matrix harness and execute the first complete source archetype through it: deterministic local file resources into DuckDB, filesystem Parquet, and Postgres destinations across append, replace, and merge where destination sheets support them.

Owns:

- `crates/cdf-conformance/src/live_run/**` or a new focused `crates/cdf-conformance/src/run_matrix/**` module;
- conformance fixture helpers needed to run file-source cells through `cdf_project::run_project`;
- matrix case/exclusion data structures;
- matrix assertion helpers for plan honesty, package validity, trait-level receipt verification, checkpoint gating, package replay identity, and duplicate behavior;
- committed evidence output for executed and excluded C1 cells.

## Acceptance Criteria

- The harness represents cells as `(source archetype, destination, disposition)` and records unsupported cells with a reason rather than silently skipping them.
- File-source cells execute through `run_project` or its generic successor, not destination-specific wrappers.
- File-source cells cover DuckDB, Parquet, and Postgres destinations.
- File-source cells cover append and replace where supported; merge cells either execute where sheets support merge or are explicitly excluded with the sheet-backed reason.
- Each executed cell asserts:
  - the plan resource id, package id, and scope match the selected resource;
  - the package verifies through `PackageReader::verify`;
  - the destination receipt verifies through the destination trait-level `verify`;
  - the checkpoint head is committed only after receipt verification;
  - package artifact replay produces identity-compatible output;
  - duplicate replay is a no-op or a documented sheet-backed behavior.
- Matrix output is persisted in an evidence record with exact executed and excluded cells.

## Evidence Expectations

Run focused C1 tests, destination-specific live dependencies when required, `cargo fmt --all --check`, `cargo check -p cdf-conformance -p cdf-project --all-targets --locked`, `cargo clippy -p cdf-conformance -p cdf-project --all-targets --locked -- -D warnings`, `cargo nextest run -p cdf-conformance --locked`, `git diff --check`, `jscpd` over touched conformance modules, `rust-code-analysis-cli` over touched conformance modules, and relevant security scans if new fixture text or secret handling is added.

## Explicit Exclusions

No REST or SQL source matrix cells; C2 owns them. No cross-destination chaos killpoint expansion; C3 owns it. No committed golden updates beyond transient matrix fixture output required for this slice; C4 owns final per-destination live goldens. No property/fuzz target work; C5 owns it.

## Progress And Notes

- 2026-07-08: Split from P0 Workstream C after Workstreams A and B closed. Existing conformance has `live-local-file-v1` for local file to DuckDB only; this child generalizes that pattern into the matrix foundation and broadens the file source across current MVP destinations/dispositions.
- 2026-07-08: Activated for worker implementation. Worker owns the conformance-owned file-source run-spine matrix foundation, focused tests, and C1 progress notes. Parent owns review, final quality gates, evidence/review records, closure, commit, and push.
- 2026-07-08: Implemented `cdf-conformance::run_matrix` with FILE-source cells for DuckDB, filesystem Parquet, and Postgres across append/replace/merge. Focused test output records 8 executed cells and 1 excluded cell: Parquet merge is excluded because its destination sheet lists supported_dispositions=[append, replace]. Worker verification passed: `cargo fmt --all --check`, `cargo test -p cdf-conformance run_matrix -- --nocapture`, `cargo check -p cdf-conformance -p cdf-project --all-targets --locked`, `cargo clippy -p cdf-conformance -p cdf-project --all-targets --locked -- -D warnings`, and `git diff --check`.
- 2026-07-08: Parent review found and repaired the initial conditional Postgres skip path; Postgres setup is now mandatory for the C1 matrix test and setup/schema failures fail loudly. Evidence is recorded in `.10x/evidence/2026-07-08-p0-c1-run-spine-matrix-foundation.md`; adversarial review is recorded in `.10x/reviews/2026-07-08-p0-c1-run-spine-matrix-foundation-review.md`. C1 is closed.

## Blockers

None.
