Status: recorded
Created: 2026-07-12
Updated: 2026-07-12
Target: crates/cdf-transform-zstd/src/lib.rs
Verdict: pass

# Zstd frame-window admission review

## Findings

- No critical or significant finding.
- Header bytes are consumed once from the accounted cursor and replayed once into the native decoder; payload bytes remain zero-copy borrowed from their accounted input chunk.
- Standard and skippable magic are distinguished before reservation. Reserved/unused descriptor bits and oversized windows fail before native decode.
- Single-segment frames use content size as window authority; ordinary frames use the zstd window descriptor formula with checked arithmetic.
- The per-frame lease is dropped at frame completion, so concatenated frames do not accumulate window reservations.
- The 4 MiB decoder-context allowance is conservative but bounded and materially smaller than the superseded fixed-window reservation for common frames.

## Verdict

Pass. Frame evidence now controls admission without weakening the 64 MiB safety ceiling.

## Residual risk

Malformed-header fuzzing and platform-specific native context measurements remain before B1 closure.
