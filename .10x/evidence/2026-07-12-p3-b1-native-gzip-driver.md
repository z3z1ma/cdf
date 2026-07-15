Status: recorded
Created: 2026-07-12
Updated: 2026-07-12
Relates-To: .10x/tickets/2026-07-11-p3-b1-streaming-byte-transforms.md, .10x/tickets/done/2026-07-11-p0-fx1-native-format-extension-boundary.md

# Native streaming gzip driver

## What was observed

`cdf-transform-gzip` implements the neutral `ByteTransformDriver` as a leaf crate. It incrementally consumes accounted compressed chunks, parses bounded gzip headers, drives raw deflate without a decoded-object buffer, verifies optional header CRC plus mandatory payload CRC/ISIZE trailers, continues across concatenated members, and emits separately accounted output chunks.

The driver retains one 128 KiB transform-class lease covering its native deflate window and bounded header parser. It reserves each output chunk before allocation. Exhausted input chunks are dropped before polling the next one, preventing a transient two-input-chunk ledger overlap. Expanded-byte, expansion-ratio, cancellation, corruption, truncation, header-size, and reserved-flag failures are fail-closed.

## Procedure

- `cargo test -p cdf-transform-gzip --locked`
  - Result: 3 unit tests and doc tests passed.
  - Covered concatenated members split into one-byte input chunks; checksum corruption; trailer truncation; expanded-byte ceiling; compression-ratio ceiling; cancellation; current-byte release; and a 131,102-byte peak bound for a 128 KiB internal lease, 29-byte output reservation, and one-byte input.
- `cargo clippy -p cdf-transform-gzip --all-targets --locked -- -D warnings`
  - Result: passed.
- Dependency inspection:
  - The crate depends only on `cdf-kernel`, `cdf-memory`, `cdf-runtime`, `flate2`, `crc32fast`, `bytes`, and `futures-util`; both third-party codec dependencies were already pinned in the workspace lock graph.

## What this supports or challenges

This supports B1's streaming, ledger, concatenated-member, checksum, truncation, expansion-safety, and cancellation architecture for gzip, and FX1's claim that transforms can be added behind the neutral registry contract without source/runtime codec leakage.

## Limits

This slice does not compose gzip into the file product path, remove the superseded `cdf-formats` full-buffer/read-adapter paths, establish the checksum-window accepted-row publication barrier, add fuzz/property corpora, or record throughput ratios. Those remain owned by the open B1 ticket; this evidence does not support closing B1 or FX1.
