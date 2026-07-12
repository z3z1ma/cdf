Status: recorded
Created: 2026-07-12
Updated: 2026-07-12
Target: crates/cdf-runtime/src/format.rs, crates/cdf-source-files/src/runtime.rs, crates/cdf-format-parquet, crates/cdf-format-arrow-ipc
Verdict: pass

# Physical decode schema authority review

## Findings

- No critical or significant finding.
- The previous hash-only request was insufficient for row codecs and allowed schema/hash authority to diverge. It was deleted rather than retained as a compatibility field.
- The effective schema runtime already owned the physical schema catalog; the source now forwards that authority rather than introducing another snapshot resolver.
- `PhysicalSchemaAuthority` is private file-runtime plumbing that prevents paired arguments from proliferating; codec-facing authority remains the single `SchemaRef`.
- Drivers still attest the live physical schema against the request-derived hash, so providing a schema reference does not weaken drift detection.

## Verdict

Pass. The contract now supports binary and row codecs without two competing schema truths.

## Residual risk

CSV/fixed-width implementation must prove it uses the supplied schema for all batches and does not silently widen/reinfer during execution.
