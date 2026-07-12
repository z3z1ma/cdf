Status: recorded
Created: 2026-07-11
Updated: 2026-07-11
Relates-To: .10x/tickets/2026-07-11-p3-a5a-graph-edge-contracts.md

# P3 A5a compiled graph and accounted-edge milestone

## What was observed

`cdf-runtime` now owns a deterministic semantic operator-graph artifact and accounted ownership-transfer envelope. `cdf-engine` compiles a planned declarative resource transition from source/destination capabilities into that artifact. The graph is exposed by plan reports and written into each package as `plan/operator-graph.json` before extraction.

The mock external source test uses the id `external_mock`, but no graph node contains that id. Its capability version and working-set/concurrency declarations determine the source node. The resulting graph contains the fused reconcile/transform edge and durable segment handoff, validates after serialization, and round-trips byte-for-byte through the package artifact.

## Procedure

- `cargo test -p cdf-runtime --lib` — 23 passed.
- `cargo test -p cdf-engine operator_graph_compiles_from_capabilities_without_driver_name_dispatch -- --nocapture` — 1 passed; the test also executes a package and reads back `plan/operator-graph.json`.
- `cargo test -p cdf-cli plan_json_exposes_pushdown_ddl_guarantee_and_state_advancement -- --nocapture` — passed.
- `cargo test -p cdf-cli run_local_file_to_duckdb_commits_package_rows_mirrors_and_checkpoint -- --nocapture` — passed.
- `cargo clippy -p cdf-runtime -p cdf-engine -p cdf-cli --all-targets -- -D warnings` — passed.

The broad `cargo test -p cdf-runtime -p cdf-engine -p cdf-cli --lib` run completed runtime and engine successfully but reported five unrelated existing CLI failures: SQL-query compatibility wording, progress-router source inspection, two unknown-destination exit-code expectations, and a promotion test lacking injected destination services. No failure referenced the graph artifact or accounted edge. These are limits, not A5a closure evidence.

## What this supports

- Graph identity is deterministic and excludes runtime queue capacity/timing.
- New source/destination implementations affect graph construction through capability declarations rather than generic name dispatch.
- Arrow/byte payload and outcome metadata ownership cannot cross the new edge without shared ledger reservations.
- The compiled graph survives planning into package evidence.

## Limits

The existing execution loop does not yet run operators through these edges. Structured first-failure task integration, architecture source scans, slow-consumer/panic conformance, and an edge-overhead benchmark remain required before A5a can close.
