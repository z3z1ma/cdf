Status: recorded
Created: 2026-07-12
Updated: 2026-07-12
Target: crates/cdf-source-files/src/runtime.rs, crates/cdf-source-files/src/lib.rs, crates/cdf-formats, crates/cdf-project/src/schema_discovery.rs
Verdict: concerns

# Registry transform execution review

## Findings

No critical findings.

The initial remote discovery implementation was rejected because it downloaded the full object even for an uncompressed bounded sample. The repaired path keeps uncompressed reads bounded and uses one bounded prefix plus a bounded private transform spool for compressed sampling.

The initial temporary spool issued `sync_data`; review rejected it because the spool is neither a package artifact nor checkpoint authority. It was removed.

Significant open finding: compressed binary discovery and local schema attestation still hand the outer compressed path to binary probes. Execution is correct and covered, but full discovery/preview/run parity is not yet complete. P3 B1 and P0 FX1 remain open owners.

## Residual risk

Discovery samples may intentionally precede terminal transform checksum. They cannot become accepted data: the snapshot binds the original source identity, and preview/run repeat the transform to terminal integrity before publishing batches. Remote execution performs two bounded disk passes until the neutral remote byte-source lane lands. No legacy decoder fallback remains.
