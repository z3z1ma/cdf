Status: recorded
Created: 2026-07-12
Updated: 2026-07-12
Relates-To: .10x/tickets/2026-07-11-p3-b1-streaming-byte-transforms.md, .10x/tickets/done/2026-07-11-p0-fx1-native-format-extension-boundary.md

# Compressed binary discovery, attestation, and execution parity

## What was observed

`cdf-source-files` now owns one composed binary probe for Parquet and Arrow IPC. Local compressed candidates reach transform terminal integrity before footer/schema parsing; remote compressed binaries spool with generation verification, transform, then use the same probe. Remote Arrow IPC discovery is no longer rejected. Local schema attestation also uses this composed path.

Project discovery removed its compressed-binary exclusion. An exhaustive two-file `.parquet.gz` fixture independently transforms both candidates, joins `int32` and `int64`, and pins the widened `int64` schema with both candidates marked probed.

The first async integration exposed a nested-runtime deadlock: remote open ran on the injected I/O runtime and synchronously called `run_io` while transforming. Sampling showed the test thread awaiting the outer stream and the I/O worker blocked in `ExecutionServices::run_io` from `spool_transformed_file`. The repaired architecture resolves/attests/downloads through the synchronous transport facade before entering the I/O task; transform and decode are async inside the task. No blocking transport or nested runtime call remains in the hot I/O worker.

## Procedure

- `cargo clippy -p cdf-source-files -p cdf-project --all-targets -- -D warnings`
- `cargo test -p cdf-source-files gzip_ -- --nocapture` (2 passed)
- `cargo test -p cdf-project object_store_gzip_ndjson_discovers_pins_and_executes_through_one_transport -- --nocapture` (passed in 0.24s after previously hanging beyond 60s)
- `cargo test -p cdf-project exhaustive_gzip_parquet_discovery_joins_every_transformed_candidate -- --nocapture` (passed)
- `cargo test -p cdf-project exhaustive_local_parquet_discovery_aggregates_widening_missing_metadata_and_set_identity -- --nocapture` (passed)

## What this supports

Discovery, attestation, and execution interpret transformed binary bytes through the same source-owned composition. Multi-file pinning is not a single-file special case. The injected runtime boundary is explicit: blocking transport preparation precedes async transform/decode.

## Limits

Remote transformed binaries still use an input spool plus verified output spool. P3 G1/G2 own replacing the synchronous transport facade with a neutral remote `ByteSource` and overlapped transfer/decode. Non-gzip driver execution composition relies on their shared registry law and leaf suites; B1 still owns matrix/fuzz evidence across every registered transform.
