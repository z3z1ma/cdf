Status: recorded
Created: 2026-07-12
Updated: 2026-07-12
Target: crates/cdf-source-files/src/runtime.rs, .10x/evidence/2026-07-12-p3-b1-streaming-transform-product-composition.md
Verdict: pass

# Streaming transform product composition review

## Target

The production join between transport byte sources, registered byte transforms, registered format drivers, and shared spill authority.

## Findings

No critical or significant defect remains in this slice.

- Pass: The runtime branches on neutral source capabilities and `FormatSourceAccess`, not transport/transform/format names. Leaf implementations remain outside the generic source runtime.
- Pass: Sequential transformed inputs never enter the disk-spool path. The permanent object-store gzip-NDJSON test proves this under a one-byte spool ceiling and a zero-spill snapshot.
- Pass: Adaptive transformed inputs materialize only transformed output. Spill authority grows before writes, the configured disk ceiling remains enforced, and the reservation lives through decode.
- Pass: Transform expansion safety is not accidentally coupled to disk capacity for direct streaming; the registered driver descriptor and ratio guard remain authoritative. The legacy materialized transform path retains the disk ceiling.
- Pass: The external sequential driver no longer lies about requiring exact ranges or known output length, strengthening rather than weakening extension conformance.

## Residual risk

Weak-provider compatibility spooling, compressed discovery spooling, growing-spool early decode, transform fuzzing, and measured remote overlap remain explicitly owned by B1/G1/G2/G3. None requires a compatibility shim in this slice.

## Verdict

Pass. The change removes a full materialization stage for strong local/remote compressed row sources and preserves one neutral composition architecture.
