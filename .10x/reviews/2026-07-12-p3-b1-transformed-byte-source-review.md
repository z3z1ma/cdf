Status: recorded
Created: 2026-07-12
Updated: 2026-07-12
Target: crates/cdf-runtime/src/transformed_byte_source.rs, crates/cdf-runtime/src/format.rs
Verdict: pass

# Transformed byte source adversarial review

## Findings

- No critical or significant finding in the neutral composition slice.
- The adapter does not leak codec, transport, path, registry, or destination concepts.
- Capability reduction is fail-closed: compressed output is never represented as seekable, exact-range-capable, known-length, or range-concurrent.
- Identity derivation changes when upstream generation/checksum or transform semantic version changes, but does not misrepresent the derivation digest as the actual output checksum.
- Output chunk authority is now explicit rather than inferred from a total working-set number.
- A passthrough test driver proves adapter behavior without introducing a runtime-to-codec dependency cycle.

## Verdict

Pass. This is the correct reusable seam for sequential registered format drivers.

## Residual risk

Random-access formats still require a separately budgeted spool adapter, and checksum-bearing transform publication must be reconciled with decoder/run atomicity. Product composition remains open under B1.
