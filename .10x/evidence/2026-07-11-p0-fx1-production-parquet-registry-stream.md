Status: recorded
Created: 2026-07-11
Updated: 2026-07-11
Relates-To: .10x/tickets/done/2026-07-11-p0-fx1-native-format-extension-boundary.md, .10x/tickets/2026-07-11-p3-b2-parquet-codec.md, .10x/tickets/done/2026-07-11-p3-a5e-streaming-graph-integration.md

# Production Parquet routes through the native registry stream

## What was observed

The CLI composition root now registers `cdf-format-parquet::ParquetFormatDriver` in the neutral `FormatRegistry` and injects the complete file runtime dependency set through `FileSourceDriver`. `cdf-source-files` asks the registry for the declared format id; any installed uncompressed native driver enters the same generic structured-I/O stream. There is no Parquet-specific executor or transport branch in that registered path.

The native producer plans deterministic decode units, emits accounted physical batches at a 64k-row/16 MiB target through a two-item bounded stream, transfers their leases into kernel payload retention, and lets the engine execute shared schema reconciliation. Local files and verified remote spools use the same driver. The dependency-free local/default runtime constructors and direct compiler-owned file-open functions were deleted; executable resources require an injected execution host and registry.

## Procedure

- `cargo test -p cdf-source-files --lib` — 15 passed.
- `cargo test -p cdf-declarative --lib` — 81 passed after executable tests were moved to resolved `FileResource` instances.
- `cargo test -p cdf-cli run_local_parquet_discover_autopins_and_commits_pinned_schema --lib -- --nocapture` — passed.
- `cargo test -p cdf-cli run_adhoc_http_parquet_uses_bounded_discovery_and_ordinary_run --lib -- --nocapture` — passed.
- `cargo clippy -p cdf-source-files -p cdf-declarative -p cdf-project -p cdf-cli --all-targets -- -D warnings` — passed.

A 150,000-row local Parquet fixture arrived as three bounded batches and held ledger memory until the consumer dropped them. A full release TLC run over `/private/tmp/yellow_tripdata_2024-01.parquet` loaded 2,964,624 rows into DuckDB in 16 canonical segments. Three fresh wall/CPU observations were 2.39/1.80, 1.55/1.65, and 1.63/1.81 seconds, for medians of 1.63 wall and 1.80 CPU seconds. The first sample's extraction-to-package-finalization event interval was approximately 1.20 seconds.

## What this supports

Production Parquet no longer executes through `cdf-formats` dispatch. Remote full scans retain the sequential verified spool policy and perform Parquet parsing locally, so the former serialized HTTP range-read pathology cannot return through this path. Preview and run resolve the same typed source and registered driver.

## Limits

This is an architecture and streaming milestone, not a throughput win. The recent pre-registry TLC control median was about 1.53 wall/1.62 CPU seconds; the new medians are approximately 6.5% and 11% higher, while measured package execution moved only from about 1.178 to 1.20 seconds. The likely remaining tax is the local neutral range provider's allocation/copy/open/attestation path versus the old parser-owned seekable file. B2 remains open to recover and exceed that CPU roofline without reintroducing raw file handles into codecs.

Unregistered CSV/JSON/NDJSON/Arrow IPC still use the old monolithic implementation. FX1 is not complete until their codec crates register and that fallback, the closed format enum, and compiler execution residue are deleted.
