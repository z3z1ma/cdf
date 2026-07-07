Status: recorded
Created: 2026-07-06
Updated: 2026-07-06
Target: .10x/tickets/done/2026-07-06-live-local-file-run-golden-conformance.md
Verdict: pass

# Live local-file run golden conformance review

## Target

Reviewed the live-run conformance implementation in `crates/firn-conformance/src/live_run/`, the crate-root export in `crates/firn-conformance/src/lib.rs`, the committed golden fixture at `crates/firn-conformance/golden/live-local-file-v1/expected.json`, and the closure evidence in `.10x/evidence/2026-07-06-live-local-file-run-golden-conformance.md`.

## Findings

No blocking findings.

- The implementation stays inside the ticket's conformance scope. It consumes `firn_project::run_local_file_to_duckdb_checkpoint` rather than duplicating production run sequencing or widening runtime behavior.
- The crate root remains thin and the new live-run logic is split into `live_run/mod.rs` and `live_run/tests.rs`, matching the current crate-organization convention.
- The golden fixture is deterministic and anchored by explicit package, checkpoint, pipeline, resource, target, source path, source hash, source size, row count, and package evidence values. The 100-run test compares regenerated package evidence to the committed fixture.
- Receipt, checkpoint, and destination assertions cover the high-risk state transitions: package status, durable package receipt, verified DuckDB receipt, committed SQLite checkpoint head, destination mirror loads/state, segment row counts, and source-position metadata.
- The post-receipt failure test covers the firn-line window at the live-run level: after receipt verification but before checkpoint commit, the source file can be removed and recovery still commits from the durable receipt.
- Duplicate replay reuses the prepared-package conformance assertions and verifies no second destination write or mirror-row inflation.
- Negative self-tests and mutation testing reduce the risk that the harness passes while skipping material checks. The mutation run over `live_run/mod.rs` had 0 missed mutants.
- The change does not alter native Parquet policy or add a dependency path to `parquet`/`paste`.

## Residual Risk

- The live fixture constructs the `EnginePlan` through deterministic JSON matching the current engine serialization shape. That is acceptable for this conformance slice because the golden gate is intended to fail on plan-shape drift, but broader runtime plan construction remains owned by the engine/declarative run surfaces.
- Geiger remains tool-limited by dependency warnings. The evidence pairs that limit with direct first-party unsafe scans and `cargo careful`.
- The parent conformance ticket still owns full lifecycle killpoints, MVP killer-demo orchestration, HTTP/API and SQL source execution conformance, boundedness honesty, and property/fuzz targets.

## Verdict

Pass. The ticket acceptance criteria are supported by implementation, focused tests, broad relevant quality gates, mutation testing, and documented tool limits.
