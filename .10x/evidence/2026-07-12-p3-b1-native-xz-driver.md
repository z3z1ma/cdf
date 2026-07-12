Status: recorded
Created: 2026-07-12
Updated: 2026-07-12
Relates-To: .10x/tickets/2026-07-11-p3-b1-streaming-byte-transforms.md, .10x/tickets/2026-07-11-p0-fx1-native-format-extension-boundary.md

# Native xz driver

## What was observed

`cdf-transform-xz` drives liblzma's low-level `.xz` stream API directly over accounted chunks. `LZMA_CONCATENATED` preserves multi-stream semantics; `Action::Finish` distinguishes valid EOF/checksum completion from truncation. Exact cumulative byte counters advance only consumed compressed bytes. CRC/integrity checks remain enabled and unsupported/corrupt checks fail.

The decoder has a hard 64 MiB liblzma memlimit and CDF reserves the same native working set before construction. `liblzma 0.4.7` is built with `default-features = false, features = ["static"]`: CDF compiles the pinned bundled xz/liblzma implementation and cannot opportunistically link an ambient host library. Bindgen and parallel encoder dependencies are disabled.

## Procedure and results

- `cargo test -p cdf-transform-xz --locked`: passed (2 correctness tests; one release benchmark ignored) against the static tuple.
- Concatenated xz streams decoded from one-byte chunks; managed peak stayed below the 64 MiB native lease plus output/input and returned to zero.
- Integrity corruption, truncation, expanded-byte ceiling, and cancellation fail closed.
- `cargo clippy -p cdf-transform-xz --all-targets --locked -- -D warnings`: passed.
- `cargo test --release -p cdf-transform-xz --locked xz_driver_reference_rate -- --ignored --nocapture`: `xz_reference_ms=29.035`, `xz_driver_ms=29.493`, ratio `0.984x`; passed the `>=0.6x` floor using the same bundled static implementation.
- `cargo deny --locked check`: all gates passed.
- `cargo vet --locked --no-minimize-exemptions`: succeeded (2 audited, 490 exempted); liblzma/liblzma-sys are exact policy exemptions, not claimed source audits.
- Dependency inspection confirmed the build script reads only the packaged source tree/environment/toolchain, sorts C inputs for reproducibility, performs no download, and compiles single-threaded liblzma when static fallback is selected.

## What this supports

XZ can stream with deterministic native implementation identity, a hard memory limit, checksum enforcement, and essentially native reference throughput.

## Limits

The bundled C implementation remains an unsafe/native supply-chain boundary. Whole-stream integrity is learned after earlier output chunks, so product composition still requires the checksum publication barrier before accepted visibility.

