Status: recorded
Created: 2026-07-14
Updated: 2026-07-14

# P3 G2 typed source-read mode telemetry

## Observation

Successful source-read phase metrics now carry the transport-neutral access strategy that produced them: `direct_stream`, `exact_ranges`, `full_spool`, `growing_spool`, or `mixed_access`. Metrics with different strategies aggregate separately, and the CLI names the strategy beside physical/useful/waste bytes, request count, and source wait.

The byte observer infers direct/range activity from the actual neutral `ByteSource` methods. The spool controller explicitly overrides that low-level activity only after it has selected a full or growing spool. Consequently a growing spool is not mislabeled as a plain sequential request, and local range decode plus a full verification sweep is honestly labeled `mixed_access`.

The mode and all source-I/O counters remain invocation-local operational telemetry. None enters a plan, package, manifest, source position, or hash.

A release smoke run in the external `/Users/alexanderbut/code_projects/tmp` project exercised five live GitHub-hosted Parquet files. It committed 5,000 rows through DuckDB and the checkpoint gate in 4.18 seconds. The ordinary progress stream rendered:

`source_read growing_spool Completed 349 KiB physical / 339 KiB useful / 9.9 KiB waste across 15 requests in 147ms`

The 9.9 KiB is generation-bound tail/footer coverage fetched before the same bytes arrived in each full spool. This run caught and corrected two fixture-blind telemetry defects: the normal-verbosity field whitelist initially hid the typed metric, and useful bytes initially counted those duplicate cross-operation bytes as useful twice.

## Procedure

- `CARGO_BUILD_JOBS=12 cargo check -p cdf-kernel -p cdf-runtime -p cdf-source-files -p cdf-engine -p cdf-project -p cdf-cli --all-targets --locked`
- `CARGO_BUILD_JOBS=12 cargo test -p cdf-kernel -p cdf-runtime --lib --locked`
- `CARGO_BUILD_JOBS=12 cargo test -p cdf-source-files remote_parquet_uses_admitted_spool_or_generation_bound_ranges --locked -- --nocapture`
- `CARGO_BUILD_JOBS=12 cargo test -p cdf-project general_project_run_records_bounded_complete_phase_telemetry --locked -- --nocapture`
- `CARGO_BUILD_JOBS=12 cargo test -p cdf-cli source_read_metric_names_physical_useful_waste_and_requests --locked`
- `CARGO_BUILD_JOBS=12 cargo build -p cdf-cli --release --locked`
- From `/Users/alexanderbut/code_projects/tmp`: `/usr/bin/time -p /Users/alexanderbut/code_projects/personal/cdf/target/release/cdf run github.userdata`

All commands passed. Kernel passed 38/38; runtime passed 48 with one explicit benchmark ignored; each focused source, project, and CLI regression passed 1/1. The final live run completed with `real 4.18`, `user 0.14`, and `sys 0.11`, published 30 run events, wrote five canonical segments, verified a five-segment DuckDB receipt, and committed the checkpoint.

## What this supports or challenges

Supports the G2/G4 requirement that spool/range/cache tuning be measured rather than inferred. It also makes the finite-object disk policy operator-visible: an oversized object using ranges is distinguishable from an admitted growing spool without inspecting source-specific implementation details.

## Limits

Retry/throttle counts, controller feedback state, disk high-water, cache decisions, and failed/partial invocation telemetry remain open. This evidence does not claim any throughput improvement by itself.
