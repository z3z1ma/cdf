Status: recorded
Created: 2026-07-12
Updated: 2026-07-12
Relates-To: .10x/tickets/done/2026-07-11-p3-b1-streaming-byte-transforms.md

# Zstd frame-window admission

## What was observed

The zstd driver no longer reserves its maximum 68 MiB decoder window for every partition. Before native decode of each frame, it incrementally parses the standard or skippable frame header, validates descriptor bits, computes the exact declared window, and reserves `window + 4 MiB` for decoder context. The lease is released at frame completion. Standard windows above 64 MiB fail before decoder construction/use.

The concatenated test now includes a skippable frame followed by two checksummed frames, all delivered as one-byte input chunks. Its measured ledger peak is 4,718,624 bytes with zero retained bytes after completion. The former conservative bound was approximately 71.3 MiB for the same output chunk, so admission consumption is about 15 times lower on this fixture.

## Procedure

- `cargo test -p cdf-transform-zstd --locked`
  - Result: 3 tests and doc tests passed.
  - Covered skippable plus concatenated frames, one-byte rechunking, checksum corruption, truncation, absolute/ratio expansion, cancellation, an explicitly encoded >64 MiB window header, peak memory, and complete lease release.
- `cargo clippy -p cdf-transform-zstd --all-targets --locked -- -D warnings`
  - Result: passed.
- `git diff --check`
  - Result: passed before commit.

## What this supports or challenges

This supports B1's constant-memory and concurrency goals: native memory is admitted from frame evidence rather than a worst-case static reservation, and malformed/oversized claims fail before allocation.

## Limits

The 4 MiB context overhead is deliberately conservative and has not yet been replaced by measured allocator telemetry. Reference throughput and fuzz-generated frame-header coverage remain B1 obligations.
