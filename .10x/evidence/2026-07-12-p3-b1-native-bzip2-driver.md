Status: recorded
Created: 2026-07-12
Updated: 2026-07-12
Relates-To: .10x/tickets/2026-07-11-p3-b1-streaming-byte-transforms.md, .10x/tickets/done/2026-07-11-p0-fx1-native-format-extension-boundary.md

# Native bzip2 driver

## What was observed

`cdf-transform-bzip2` drives bzip2 0.6.1's incremental decompressor directly over accounted input/output. Exact cumulative byte counters preserve unconsumed trailing bytes across concatenated members. CRC/data/magic failures are reported by the decoder; EOF, native memory exhaustion, stalls, expansion, and cancellation fail closed. The fast decoder is constructed only after an 8 MiB native working-set lease covering the format's maximum 900 KiB block mode.

The bzip2 crate defaults to Trifecta Tech Foundation's Rust `libbz2-rs-sys 0.2.5`, not a system C library. Its algorithm modules forbid unsafe code; its compatibility/allocator ABI contains explicit pointer/allocator unsafe blocks. The package has no build script or network/process capability. Its `bzip2-1.0.6` license permits source/binary redistribution with attribution and non-endorsement conditions and is now explicitly allowlisted.

## Procedure and results

- `cargo test -p cdf-transform-bzip2 --locked`: passed (2 correctness tests; one release benchmark ignored).
- Concatenated members decoded from one-byte upstream chunks; managed peak stayed below the 8 MiB working lease plus one output/input chunk and returned to zero.
- CRC/data corruption, truncation, expanded-byte ceiling, and cancellation fail closed.
- `cargo clippy -p cdf-transform-bzip2 --all-targets --locked -- -D warnings`: passed.
- `cargo test --release -p cdf-transform-bzip2 --locked bzip2_driver_reference_rate -- --ignored --nocapture`: `bzip2_reference_ms=344.227`, `bzip2_driver_ms=345.136`, ratio `0.997x`; passed the `>=0.6x` floor.
- `cargo deny --locked check`: all gates passed after explicit `bzip2-1.0.6` admission.
- `cargo vet --locked --no-minimize-exemptions`: succeeded (2 audited, 488 exempted); exact bzip2 packages are recorded exemptions, not claimed full audits.

## What this supports

Bzip2 can stream at essentially native reference speed through the neutral transform contract without a blocking-reader adapter, system library dependency, or expanded-object buffering.

## Limits

The 8 MiB reservation is a conservative format/native bound and remains subject to RSS falsification. Cargo Vet records policy acceptance, not a line-by-line proof of the unsafe compatibility layer.
