Status: recorded
Created: 2026-07-12
Updated: 2026-07-12
Relates-To: .10x/tickets/2026-07-11-p3-b1-streaming-byte-transforms.md, .10x/tickets/done/2026-07-11-p0-fx1-native-format-extension-boundary.md

# Native streaming zstd driver

## What was observed

`cdf-transform-zstd` implements the neutral byte-transform contract using zstd's raw streaming operation. It accepts accounted input at arbitrary boundaries, emits separately reserved chunks, continues across concatenated frames, lets the native decoder verify optional frame checksums, rejects truncated/corrupt frames, and shares runtime expansion/cursor semantics with gzip.

The driver configures a 64 MiB maximum zstd window and retains a 68 MiB transform-class lease for the window plus decoder context. Output chunks are admitted separately. The one-byte-input test observed no retained memory after completion and a peak no greater than the internal lease plus one input byte and one 31-byte output reservation.

## Procedure

- `cargo test -p cdf-transform-zstd --locked`
  - Result: 3 unit tests and doc tests passed.
  - Covered concatenated checksummed frames, one-byte input rechunking, checksum corruption, truncation, expanded-byte and ratio limits, cancellation, peak/current ledger state.
- `cargo clippy -p cdf-transform-zstd --all-targets --locked -- -D warnings`
  - Result: passed.
- `cargo check -p cdf-transform-zstd --offline`
  - Result: passed while updating the lock graph for the new workspace member; no new third-party package version was introduced.

## What this supports or challenges

This supports B1's zstd streaming/correctness/ledger slice and demonstrates that the FX1 neutral transform seam serves two materially different framing implementations without codec logic in the source or runtime.

## Limits

The 68 MiB native reservation is conservative per partition. B1 still owes exact frame-window admission and throughput measurement. Product composition, legacy full-buffer deletion, fuzz/property corpora, and the remaining catalog transforms are also outside this milestone.
