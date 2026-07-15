Status: recorded
Created: 2026-07-14
Updated: 2026-07-14

# P3 G2 typed source-read mode telemetry

## Observation

Successful source-read phase metrics now carry the transport-neutral access strategy that produced them: `direct_stream`, `exact_ranges`, `full_spool`, `growing_spool`, or `mixed_access`. Metrics with different strategies aggregate separately, and the CLI names the strategy beside physical/useful/waste bytes, request count, and source wait.

The byte observer infers direct/range activity from the actual neutral `ByteSource` methods. The spool controller explicitly overrides that low-level activity only after it has selected a full or growing spool. Consequently a growing spool is not mislabeled as a plain sequential request, and local range decode plus a full verification sweep is honestly labeled `mixed_access`.

The mode and all source-I/O counters remain invocation-local operational telemetry. None enters a plan, package, manifest, source position, or hash.

## Procedure

- `CARGO_BUILD_JOBS=12 cargo check -p cdf-kernel -p cdf-runtime -p cdf-source-files -p cdf-engine -p cdf-project -p cdf-cli --all-targets --locked`
- `CARGO_BUILD_JOBS=12 cargo test -p cdf-kernel -p cdf-runtime --lib --locked`
- `CARGO_BUILD_JOBS=12 cargo test -p cdf-source-files remote_parquet_uses_admitted_spool_or_generation_bound_ranges --locked -- --nocapture`
- `CARGO_BUILD_JOBS=12 cargo test -p cdf-project general_project_run_records_bounded_complete_phase_telemetry --locked -- --nocapture`
- `CARGO_BUILD_JOBS=12 cargo test -p cdf-cli source_read_metric_names_physical_useful_waste_and_requests --locked`

All commands passed. Kernel passed 38/38; runtime passed 48 with one explicit benchmark ignored; each focused source, project, and CLI regression passed 1/1.

## What this supports or challenges

Supports the G2/G4 requirement that spool/range/cache tuning be measured rather than inferred. It also makes the finite-object disk policy operator-visible: an oversized object using ranges is distinguishable from an admitted growing spool without inspecting source-specific implementation details.

## Limits

Retry/throttle counts, controller feedback state, disk high-water, cache decisions, and failed/partial invocation telemetry remain open. This evidence does not claim any throughput improvement by itself.
