Status: recorded
Created: 2026-07-12
Updated: 2026-07-12
Target: crates/cdf-source-files/src/runtime.rs
Verdict: pass

# FX1 external codec and transform law review

## Findings

No critical or significant unresolved finding.

The mock implementations exist only under `#[cfg(test)]`; production code does not branch on their ids, extensions, framing, or schema. The transform retains accounted input without copying, and the format reserves output before publication. Discovery and decode use the same registered object. The test checks actual source position and ledger release rather than only registry membership.

The law is intentionally local. Claiming remote closure from the current synchronous transport/spool path would hide G1's missing neutral provider; the ticket remains open for that law and project-level composition.

## Verdict

Pass. The test materially falsifies the key extension claim while preserving honest remaining scope.

## Residual risk

The generic format confirmation path still relies on explicit declarations for unknown driver magic. Descriptor-driven confirmation remains open before inferred external formats can be supported.

