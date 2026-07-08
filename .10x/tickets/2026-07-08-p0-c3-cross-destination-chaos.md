Status: open
Created: 2026-07-08
Updated: 2026-07-08
Parent: .10x/tickets/2026-07-07-p0-workstream-c-spine-conformance-harness.md
Depends-On: .10x/tickets/2026-07-08-p0-c1-run-spine-matrix-foundation.md

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

## Blockers

C1 should land first if this child reuses matrix destination/source fixtures. If chaos can safely reuse existing package replay fixtures first, record that in progress before implementing.
