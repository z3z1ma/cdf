Status: recorded
Created: 2026-07-11
Updated: 2026-07-11
Relates-To: .10x/tickets/done/2026-07-11-p3-a5a-graph-edge-contracts.md

# P3 A5a compiled graph and accounted-edge milestone

## What was observed

`cdf-runtime` now owns a deterministic semantic operator-graph artifact and accounted ownership-transfer envelope. `cdf-engine` compiles a planned declarative resource transition from source/destination capabilities into that artifact. The graph is exposed by plan reports and written into each package as `plan/operator-graph.json` before extraction.

The mock external source test uses the id `external_mock`, but no graph node contains that id. Its capability version and working-set/concurrency declarations determine the source node. The resulting graph contains the fused reconcile/transform edge and durable segment handoff, validates after serialization, and round-trips byte-for-byte through the package artifact.

The neutral edge is generic over operator-owned typed outcome payloads. It separately accounts Arrow/byte data and outcome/control memory, carries optional typed source-position authority, and carries observed/effective/coercion schema hashes. Destination capability sheets select operation-specific blocking lanes; declaration order has no routing meaning.

## Procedure

- `cargo test -p cdf-runtime --lib` — 23 passed.
- `cargo test -p cdf-engine operator_graph_compiles_from_capabilities_without_driver_name_dispatch -- --nocapture` — 1 passed; the test also executes a package and reads back `plan/operator-graph.json`.
- `cargo test -p cdf-cli plan_json_exposes_pushdown_ddl_guarantee_and_state_advancement -- --nocapture` — passed.
- `cargo test -p cdf-cli run_local_file_to_duckdb_commits_package_rows_mirrors_and_checkpoint -- --nocapture` — passed.
- `cargo clippy -p cdf-runtime -p cdf-engine -p cdf-cli --all-targets -- -D warnings` — passed.
- After wakeable cancellation landed, `cargo test -p cdf-runtime --lib` — 24 passed, including a full-channel slow-consumer cancellation test; `cargo clippy -p cdf-runtime --all-targets -- -D warnings` — passed.
- Final neutral-runtime run: 26 passed, one explicitly ignored performance benchmark. Final engine run: 82 passed, three explicitly ignored performance/stress tests. DuckDB: 21 passed; Parquet: 27 passed; Postgres: 30 passed. Strict Clippy passed across runtime, engine, all three destinations, and their dependent composition/conformance crates.
- Direct release test-binary benchmark: `CDF_A5_EDGE_BENCH_ITEMS=1000000 <cdf_runtime-test> accounted_edge_overhead_benchmark --ignored --nocapture` under `/usr/bin/time -l`. Direct accounted-envelope clone: 92.75 ns/item. Bounded accounted edge: 190.05 ns/item. Incremental edge: 97.30 ns/item. Wall time: 0.29 s. Maximum RSS: 7,012,352 bytes. Page faults: 0. Swaps: 0.

The broad `cargo test -p cdf-runtime -p cdf-engine -p cdf-cli --lib` run completed runtime and engine successfully but reported five unrelated existing CLI failures: SQL-query compatibility wording, progress-router source inspection, two unknown-destination exit-code expectations, and a promotion test lacking injected destination services. No failure referenced the graph artifact or accounted edge. These are limits, not A5a closure evidence.

## What this supports

- Graph identity is deterministic and excludes runtime queue capacity/timing.
- New source/destination implementations affect graph construction through capability declarations rather than generic name dispatch.
- Arrow/byte payload and outcome metadata ownership cannot cross the new edge without shared ledger reservations.
- The compiled graph survives planning into package evidence.

## Limits

The existing business execution loop does not yet run its transform/package stages through these edges; that production migration is owned by A5b/A5c/A5e. This evidence establishes the graph/edge/host contract, not end-to-end streaming overlap.
