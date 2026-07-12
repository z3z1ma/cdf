Status: recorded
Created: 2026-07-11
Updated: 2026-07-11
Relates-To: .10x/tickets/2026-07-11-p3-g1-streaming-transport-byte-sources.md, .10x/tickets/2026-07-11-p0-fx1-native-format-extension-boundary.md

# Local generation-bound accounted byte source

## What was observed

`cdf-source-files::LocalByteSource` now implements the neutral asynchronous `ByteSource` contract directly. Sequential reads and independently opened exact ranges reserve source memory before allocation, transfer their `Vec<u8>` allocation zero-copy into `Bytes`, retain the lease with the payload, honor cancellation, and reattest the opened file generation before and after relevant reads.

On Unix, generation authority includes canonical path, device, inode, size, modification nanoseconds, and change-time seconds/nanoseconds. The neutral identity now declares `Weak | Strong | ContentAddressed` strength explicitly; the Parquet driver refuses random-access execution over weak identity and names verified sequential spool as the remediation.

The source leaf's test graph no longer depends on `cdf-engine` merely to obtain an I/O runtime. A focused two-worker test host implements the neutral execution interface inside the test build, eliminating DataFusion from `cargo tree -p cdf-source-files`.

## Procedure

- `cargo test -p cdf-source-files local_byte_source --lib`
- `cargo test -p cdf-format-parquet --lib`
- `cargo clippy -p cdf-source-files --all-targets -- -D warnings`
- `cargo clippy -p cdf-format-parquet --all-targets -- -D warnings`
- `cargo tree -p cdf-source-files | rg 'datafusion|cdf-engine'` (no matches)

All tests/checks passed on 2026-07-11. The local-source test reads a 20,000-byte file through 8 KiB streaming chunks and an exact range, observes zero residual ledger bytes, mutates the file generation, and confirms the next range fails without leaked memory.

## What this supports or challenges

This supports one transport implementation with no synchronous transport mutex, private runtime, complete-file materialization, or eager whole-file checksum. It proves the new Parquet codec can receive strong local random-access authority without learning filesystem types.

## Limits

Production file resolution has not yet selected this provider through the format registry. HTTP/object-store providers, conditional generation binding, streaming listings, and deletion of the synchronous facade remain open G1/FX1 work.
