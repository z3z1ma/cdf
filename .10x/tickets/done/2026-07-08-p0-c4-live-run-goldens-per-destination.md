Status: done
Created: 2026-07-08
Updated: 2026-07-08
Parent: .10x/tickets/done/2026-07-07-p0-workstream-c-spine-conformance-harness.md
Depends-On: .10x/tickets/done/2026-07-08-p0-c1-run-spine-matrix-foundation.md

# P0 C4: Live-run goldens per destination

## Scope

Commit and verify at least one live-run golden fixture per MVP destination: DuckDB, filesystem Parquet, and Postgres. The fixtures must be generated from live `run_project` cells and compare package evidence hash-by-hash.

Owns:

- golden fixture files under `crates/cdf-conformance/golden/**`;
- conformance golden helpers for live-run destination variants;
- evidence records containing new golden hashes and fixture provenance.

## Acceptance Criteria

- DuckDB, Parquet, and Postgres each have at least one committed live-run golden fixture.
- Each fixture records package id, package hash, checkpoint id, destination target, source position evidence, segment count, destination row counts, and package evidence.
- Golden comparison verifies the package before comparing evidence.
- Determinism is proven by repeated rebuilds/reruns with stable hashes for each committed fixture.
- Golden updates caused by ratified artifact changes are explained in evidence; accidental churn fails the harness.

## Evidence Expectations

Run focused golden tests for each destination, repeated determinism runs, `cargo test -p cdf-conformance golden --locked --no-fail-fast` or equivalent focused target, `cargo fmt --all --check`, `cargo check -p cdf-conformance --all-targets --locked`, `cargo clippy -p cdf-conformance --all-targets --locked -- -D warnings`, `git diff --check`, and an evidence record listing new hashes and fixture paths.

## Explicit Exclusions

No new runtime behavior. No full matrix expansion beyond selecting representative live-run cells already covered by C1/C2. No package archive or cross-OS CI gate.

## Progress And Notes

- 2026-07-08: Split from P0 Workstream C. Existing `live-local-file-v1` covers local file to DuckDB; this child must preserve or supersede it and add Parquet/Postgres live-run goldens.
- 2026-07-08: Activated after C3 closed. Existing `crates/cdf-conformance/src/live_run` is DuckDB-specific and already has a committed 100-run live local-file golden; implementation should either preserve it as the DuckDB fixture or migrate it into a non-DuckDB-specific live-run golden module while adding filesystem Parquet and Postgres fixtures generated through `run_project`.
- 2026-07-08: Implemented per-destination live-run golden fixtures. Preserved and migrated `crates/cdf-conformance/golden/live-local-file-v1/expected.json` as the DuckDB fixture, added `live-local-file-parquet-v1` and `live-local-file-postgres-v1`, and split test-only destination/evidence helpers under `crates/cdf-conformance/src/live_run/`. Determinism checks now run DuckDB 100x, Parquet 100x, and Postgres 10x; Postgres is bounded because each repeat resets and exercises a real database schema. Verification run by worker: `cargo test --locked -p cdf-conformance live_run -- --nocapture`, `cargo test --locked --no-fail-fast -p cdf-conformance golden`, `cargo nextest run --locked -p cdf-conformance live_run`, `cargo check -p cdf-conformance --all-targets --locked`, `cargo clippy -p cdf-conformance --all-targets --locked -- -D warnings`, `cargo fmt --all --check`, `git diff --check`, `jscpd` over changed live-run/golden paths, `rust-code-analysis-cli` over `src/live_run`, `gitleaks` over changed live-run/golden paths, and Semgrep over changed live-run/golden paths; all passed, with `jscpd` reporting expected duplication only in committed JSON golden evidence.
- 2026-07-08: Parent review accepted C4 and recorded evidence in `.10x/evidence/2026-07-08-p0-c4-live-run-goldens-per-destination.md` plus adversarial review in `.10x/reviews/2026-07-08-p0-c4-live-run-goldens-per-destination-review.md`. Source `jscpd` is clean; JSON golden fixture duplication is intentional committed evidence repetition.

## Blockers

None.
