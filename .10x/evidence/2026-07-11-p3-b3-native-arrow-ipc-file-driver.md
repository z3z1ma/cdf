Status: recorded
Created: 2026-07-11
Updated: 2026-07-11
Relates-To: .10x/tickets/2026-07-11-p3-b3-arrow-ipc-codecs.md, .10x/tickets/2026-07-11-p0-fx1-native-format-extension-boundary.md

# Native Arrow IPC file-driver milestone

## What was observed

`cdf-format-arrow-ipc` implements Arrow IPC file framing behind the neutral `FormatDriver`/`ByteSource` contracts. Discovery reads only the ten-byte trailer plus the declared footer, rejecting discovery budgets that cannot contain those bytes. Decode validates the planned schema hash, reads dictionaries and record batches by footer extents, supports exact column projection, and yields each block independently. Source `AccountedBytes` become owner-backed Arrow buffers, so the memory lease follows every zero-copy array across the runtime boundary.

The production file source selects this driver from the injected registry for local files and verified remote spools. The old local file-source IPC reader branch and remote IPC rejection were removed. A memory object-store fixture proves remote spool-to-driver execution.

## Procedure

- `cargo test -p cdf-format-arrow-ipc --lib`
- `cargo test -p cdf-source-files --lib`
- `cargo test -p cdf-declarative inferred_parquet_and_arrow_preview_and_run_share_resolved_format --lib`
- `cargo test -p cdf-cli preview_succeeds_for_csv_json_parquet_and_arrow_ipc_file_resources --lib`
- `cargo clippy -p cdf-format-arrow-ipc -p cdf-source-files -p cdf-declarative -p cdf-project -p cdf-cli --all-targets -- -D warnings`
- `cargo test --release -p cdf-format-arrow-ipc arrow_ipc_driver_reference_rate --lib -- --ignored --nocapture`

The release comparison generated a 64 MiB in-memory IPC file with 64 record batches of 65,536 rows. It reported Arrow high-level `FileReader` construction at 8,196.51 MiB/s and the driver at 471,540.98 MiB/s, ratio 57.529. Both paths returned identical row counts and the memory ledger returned to zero after retained batches dropped.

## What this supports or challenges

This supports the neutral driver boundary, bounded file framing, production local/remote parity, and zero-copy block ownership. It demonstrates that a second binary codec can be added without editing the generic registered-format execution algorithm.

## Limits

The throughput comparison is deliberately construction-path-biased: the source is in memory and consumers count rows without scanning every value. It demonstrates avoided block-buffer copies, not disk, network, or end-to-end throughput. Arrow IPC stream framing, compressed dictionaries/malformed/fuzz corpus, and storage-backed profiles remain B3 work. The generic file source still has legacy fallback dispatch for formats not yet migrated and format-specific schema attestation; FX1 owns their deletion.
