Status: recorded
Created: 2026-07-12
Updated: 2026-07-12
Relates-To: .10x/tickets/done/2026-07-11-p0-fx1-native-format-extension-boundary.md, .10x/tickets/2026-07-11-p3-b2-parquet-codec.md, .10x/tickets/2026-07-11-p3-g1-streaming-transport-byte-sources.md

# Registry-driven remote discovery

## What was observed

The project discovery compiler no longer branches on `format == parquet`. Local and remote binary discovery resolve one registered `FormatDriver`, open an injected byte source, compose an optional registered transform, and invoke the driver's bounded discovery contract. Strong native HTTP/object-store sources retain their own generation-bound range/sequential behavior. A narrow range-discovery byte source adapts older transport implementations only when they expose a strong ETag/version, reserves before each bounded range, and reattests the planned generation before and after the read.

`PhysicalSchemaObservation` now carries collision-checked format evidence. The Parquet driver publishes deterministic row count, row-group count, and canonical footer fingerprint from the metadata it already parsed; source/runtime identity keys cannot be overwritten by a driver.

The superseded `cdf-formats` Parquet discovery implementation, its `RangeChunkReader`, declarative Parquet-specific probe types/functions, project compiler branch, and their old-behavior tests were deleted: 659 removed lines versus 200 added lines in the complete slice. The registered driver is now the sole Parquet discovery implementation.

## Procedure

- `cargo check --workspace --all-targets`: passed; two pre-existing `cdf-subprocess` test `unused_mut` warnings were observed outside this change.
- `cargo test -p cdf-source-files --lib`: 27 passed, 0 failed.
- `cargo test -p cdf-format-parquet --lib`: 1 passed, 0 failed.
- `cargo test -p cdf-project tests::http_parquet_schema_discovery_uses_bounded_ranges_without_artifacts --lib -- --exact`: passed.
- `cargo test -p cdf-project tests::object_store_multi_file_parquet_discovery_pins_one_reconciled_snapshot --lib -- --exact`: passed.
- `cargo clippy -p cdf-source-files -p cdf-format-parquet -p cdf-project --all-targets -- -D warnings`: passed.
- `git diff --check`: passed.

## What this supports

- Adding or changing a binary format no longer requires project/declarative discovery dispatch edits.
- Parquet discovery still uses bounded footer ranges while execution policy remains independently adaptive.
- Multi-file object-store discovery retains deterministic reconciliation and format-specific evidence.
- One authoritative Parquet parser/fingerprint path replaces parallel native and compatibility implementations.

## Limits

Sequential providers that do not yet implement `open_byte_source` retain one verified compatibility spool. Compressed non-random-access formats still require one transformed-output spool for discovery. This record does not close live-cloud, cancellation-chaos, high-cardinality listing, or discovery throughput/RSS evidence.
