Status: recorded
Created: 2026-07-12
Updated: 2026-07-12
Relates-To: .10x/tickets/2026-07-11-p3-b1-streaming-byte-transforms.md, .10x/tickets/2026-07-11-p0-fx1-native-format-extension-boundary.md

# Registry-driven transform execution and legacy compression deletion

## What was observed

File compression is now a registry id rather than a closed gzip/zstd enum. Auto selection joins registered extension and strong-magic evidence, rejects disagreement, admits extension-only drivers only when they declare no strong magic, and records the selected transform id in partition metadata. Inner `.parquet` and `.arrow` extensions survive an outer transform suffix.

Execution downloads a remote object at most once into its transport spool, streams any selected transform through `TransformedByteSource` under the shared memory ledger, and publishes format batches only after the transform reaches terminal integrity and the verified output spool is complete. `.parquet.gz` then enters the same registered Parquet driver as uncompressed Parquet. Temporary spools flush userspace buffers but deliberately do not issue a durability sync: package/checkpoint durability begins later and a device flush here would strengthen no guarantee.

Discovery uses the same registry selection. Uncompressed remote row discovery retains bounded sequential sampling. Compressed row discovery reads only a bounded compressed prefix and produces a bounded private transformed sample; it may stop before terminal checksum because discovery observations are provisional, while preview/run keep the terminal-integrity publication barrier.

The old `cdf-formats::FileCompression`, gzip/zstd decoder branches, compression-bearing `FileSource`, compressed-format rejection, `flate2`/`zstd` direct dependencies, and the deprecated single-file Parquet project discovery helper/test were deleted. Row discovery moved from the declarative compiler to the file-source crate.

## Procedure

- `cargo clippy -p cdf-runtime -p cdf-formats -p cdf-source-files -p cdf-declarative -p cdf-project -p cdf-cli --all-targets -- -D warnings` (passed; final scoped rerun also passed)
- `cargo test -p cdf-source-files gzip_ -- --nocapture` (2 passed: remote gzip NDJSON and local gzip Parquet)
- `cargo test -p cdf-cli schema_discover_local_parquet_reports_schema_without_project_writes -- --nocapture` (passed)
- `cargo test -p cdf-formats --lib` (35 passed)
- `cargo tree -p cdf-formats -e normal --depth 1` (no direct gzip/zstd decoder dependency)
- Static search found no `FileCompression`, `with_compression`, `flate2::`, or `zstd::stream` execution surface in `cdf-formats`.

## What this supports

Adding a transform no longer edits declarations, source selection, discovery dispatch, or format decoding. Compression and character implementations terminate at the product registry; the generic file source consumes only descriptors and trait objects. Existing remote gzip NDJSON identity semantics remain intact, and transformed binary formats are no longer rejected at execution.

## Limits

Compressed Parquet/Arrow discovery and schema attestation still need the composed binary probe path before P3 B1/P0 FX1 can close. Remote transformed execution currently has a compressed transport spool followed by a verified output spool until P3 G1 supplies a neutral remote `ByteSource`; this is constant-memory but not the final overlapped-I/O shape. The `cdf-formats` test target still pulls the conformance/DataFusion/DuckDB dev graph, evidence for FX1's build-domain acceptance criterion.
