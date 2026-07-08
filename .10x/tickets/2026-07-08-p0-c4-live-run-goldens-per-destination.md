Status: open
Created: 2026-07-08
Updated: 2026-07-08
Parent: .10x/tickets/2026-07-07-p0-workstream-c-spine-conformance-harness.md
Depends-On: .10x/tickets/2026-07-08-p0-c1-run-spine-matrix-foundation.md

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

## Blockers

C1 should land first so destination-specific golden fixtures can reuse the matrix source/destination fixture setup.
