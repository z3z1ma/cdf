Status: done
Created: 2026-07-08
Updated: 2026-07-08
Parent: .10x/tickets/done/2026-07-07-p0-workstream-c-spine-conformance-harness.md
Depends-On: .10x/tickets/done/2026-07-08-p0-c1-run-spine-matrix-foundation.md, .10x/tickets/done/2026-07-08-p0-c2-rest-sql-run-matrix.md

# P0 C3: Cross-destination generic chaos

## Scope

Run the ratified crash windows through the Workstream-B generic runtime stage seam for DuckDB, filesystem Parquet, and Postgres destinations instead of DuckDB-only failpoint wrappers.

Owns:

- conformance chaos helpers under `crates/cdf-conformance/**`;
- generic `RuntimeStage` failpoint mapping in conformance;
- helper-process or equivalent durable-state crash simulation where required;
- per-destination chaos evidence.

## Acceptance Criteria

- Chaos uses the generic runtime stage seam, not destination-specific replay wrappers.
- Ratified crash windows include at least:
  - package replay verified / before destination write;
  - checkpoint proposed / before destination write;
  - durable destination receipt recorded and verified / before checkpoint commit;
  - checkpoint committed / before package status checkpointed.
- DuckDB, Parquet, and Postgres each have chaos coverage or a record-backed exclusion for an unsupported physical crash mode.
- Recovery after each crash terminates with no source contact and no checkpoint cursor ahead of durable data.
- Duplicate/retry behavior after crash does not create a second destination write unless the destination sheet explicitly requires a different recorded behavior.

## Evidence Expectations

Record chaos output per destination and crash window, focused conformance tests, any helper-process command output, `cargo fmt --all --check`, `cargo check -p cdf-conformance -p cdf-project --all-targets --locked`, `cargo clippy -p cdf-conformance -p cdf-project --all-targets --locked -- -D warnings`, `cargo nextest run -p cdf-conformance --locked`, `git diff --check`, and mutation testing over new chaos harness logic where feasible.

## Explicit Exclusions

No new product runtime behavior except narrow test hooks if the existing generic stage seam cannot express a required ratified crash window. Any such hook need must be recorded before editing production runtime. No resident streaming supervisor or CDC chaos.

## Progress And Notes

- 2026-07-08: Split from P0 Workstream C. Existing conformance covers DuckDB/SQLite lifecycle chaos; this child broadens the same invariants through the generic seam and across Parquet/Postgres.
- 2026-07-08: Activated for worker implementation after C2 closed. Start from the current run-matrix fixtures and the existing package-replay helper-process chaos harness, but drive the crash windows through the generic `RuntimeStage` seam and current generic replay/recovery APIs.
- 2026-07-08: Implemented focused `cdf-conformance::runtime_chaos` modules for destination footprints, package fixtures, helper-process stage exits, and the cross-destination output test. The helper exits through public `RuntimeStage` variants for all four ratified windows across DuckDB, filesystem Parquet, and Postgres; the checkpoint-proposed/before-destination-write window fires at `DestinationWriteReady`, matching the existing DuckDB lifecycle failpoint after package status becomes `Loading`. No production runtime files or destination-specific replay wrappers were edited. Recovery uses generic package artifact replay/recovery APIs; the no-receipt checkpoint-proposed physical crash uses `replay_prepared_package` with a replacement checkpoint id so the crashed proposed row remains non-head without adding product runtime behavior. Focused output records 12 executed cases, no exclusions, source-free recovery, checkpoint-not-ahead assertions, and duplicate retry footprint equality. Worker checks passed before parent review fix: `cargo fmt --all --check`; `cargo test -p cdf-conformance runtime_chaos -- --nocapture`; `cargo check -p cdf-conformance -p cdf-project --all-targets --locked`; `cargo clippy -p cdf-conformance -p cdf-project --all-targets --locked -- -D warnings`; `git diff --check`; `jscpd crates/cdf-conformance/src/runtime_chaos --reporters json,console --output target/quality/reports/jscpd-p0-c3-runtime-chaos --ignore "**/target/**,**/.git/**,**/reports/**"`; and `rust-code-analysis-cli -m -O json -p crates/cdf-conformance/src/runtime_chaos > target/quality/reports/rust-code-analysis-p0-c3-runtime-chaos.json`.
- 2026-07-08: Parent review adjusted the checkpoint-proposed crash trigger from `CheckpointProposed` to `DestinationWriteReady` to preserve the existing named failpoint semantics from `.10x/tickets/done/2026-07-06-local-duckdb-lifecycle-chaos-failpoints.md`.
- 2026-07-08: Closed with evidence `.10x/evidence/2026-07-08-p0-c3-cross-destination-chaos.md` and review `.10x/reviews/2026-07-08-p0-c3-cross-destination-chaos-review.md`.

## Blockers

None.
