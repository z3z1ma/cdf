Status: recorded
Created: 2026-07-12
Updated: 2026-07-12
Relates-To: .10x/tickets/2026-07-11-p3-g1-streaming-transport-byte-sources.md, .10x/tickets/2026-07-11-p3-g2-range-readahead-spool-controller.md, .10x/tickets/2026-07-11-p3-b2-parquet-codec.md

# Accounted async full-scan spool

## What was observed

Strong-generation adaptive full scans no longer call the synchronous `FileTransport::download_to_path` compatibility API. Source preparation carries the injected `ByteSource` into the I/O scope, reserves the complete planned object size from the shared spill coordinator, performs one accounted sequential stream into an owner-only temporary file with async writes, hashes while writing, verifies exact length and any planned SHA-256, then opens the local seekable source for decode. The spill reservation lives exactly as long as the spool and releases after the decode stream drops.

Sequential formats still bypass disk and consume the same remote byte source directly. Weak HTTP identity and transformed inputs retain the verified compatibility spool until their separate contracts are migrated.

## Procedure

- `cargo test -p cdf-source-files remote_parquet_full_scan_uses_verified_sequential_spool -- --nocapture` — a 100,000-row object-store Parquet file selected `PreparedFileInput::SpoolSource`, completed, recorded spill peak at least equal to source bytes, and returned spill/current memory to zero.
- `cargo test -p cdf-source-files remote_arrow_ipc_file_streams_directly_through_registered_driver -- --nocapture` — sequential Arrow IPC continued to run with a one-byte spool ceiling.
- `cargo clippy -p cdf-source-files -p cdf-transport-http -p cdf-cli --all-targets -- -D warnings`.

## What this supports

The correct full-scan policy is now also nonblocking and disk-budgeted for strong HTTP/object-store generations. Network buffers remain governed by the byte-source ledger, disk by the shared spill coordinator, and no format/provider-specific branch was added.

## Limits

Decode starts only after spool completion. Growing-spool decode, weak HTTP async transfer, transforms without double staging, cancellation chaos, fsync/cache promotion, telemetry, and measured public-TLC overlap remain open.
