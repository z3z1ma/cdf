Status: recorded
Created: 2026-07-12
Updated: 2026-07-12
Relates-To: .10x/tickets/2026-07-11-p3-b1-streaming-byte-transforms.md, .10x/tickets/2026-07-11-p0-fx1-native-format-extension-boundary.md

# Native character transforms

## What was observed

`cdf-transform-character` provides dependency-free registered transform drivers for `text_auto`, `utf8`, `utf16le`, `utf16be`, `windows1252`, and `iso8859_1`. Auto recognizes UTF-8/UTF-16 BOMs and otherwise pins UTF-8. Explicit modes strip a matching BOM and fail on conflicts. UTF-8 validation retains at most three incomplete bytes; UTF-16 handles split code units/surrogate pairs and rejects odd/unpaired input; Windows-1252 rejects its five undefined byte values; ISO-8859-1 maps all 256 values exactly. No mode performs replacement decoding.

Every output is normalized UTF-8 in requested-size accounted chunks. Input is consumed in bulk for UTF-8 and bounded scalar units for transcoding; internal carry is at most four bytes. Expansion and cancellation use the same transform authority as compression.

## Procedure and results

- `cargo test -p cdf-transform-character --locked`: passed (2 correctness tests; one release benchmark ignored).
- One-byte rechunking passed for UTF-16LE auto/BOM, UTF-16BE explicit/BOM, Windows-1252, and ISO-8859-1, including a split supplementary-plane surrogate pair.
- BOM conflict, incomplete UTF-8, invalid UTF-16 surrogate sequence, and undefined Windows-1252 byte fail with encoding/byte remediation and no replacement.
- `cargo clippy -p cdf-transform-character --all-targets --locked -- -D warnings`: passed.
- The initial UTF-8 benchmark was rejected at 0.238x because its fixture copied every input chunk and collected every output into a second full object while the reference copied once. The accepted streaming-equivalent comparison uses zero-copy source slices and consumes each materialized output chunk once: `utf8_reference_ms=7.876`, `utf8_driver_ms=3.401`, ratio `2.316x` on 64 MiB. The advantage reflects bounded streaming output lifetime versus a full retained reference allocation; it is bias-labeled and not a claim that UTF-8 validation exceeds memory bandwidth.
- `cargo deny --locked check` and Cargo Vet pass. The crate adds no third-party dependency.

## What this supports

All catalog-v1 character encodings can compose before every text codec through one neutral byte-transform contract with deterministic BOM authority and fail-closed decoding.

## Limits

The benchmark covers the UTF-8 fast path; UTF-16 and single-byte transcoding still need macro text-codec envelope coverage. Product registry/catalog wiring remains open.

