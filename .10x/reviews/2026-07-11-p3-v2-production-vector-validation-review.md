Status: recorded
Created: 2026-07-11
Updated: 2026-07-11
Target: crates/cdf-contract/src/vector.rs, crates/cdf-engine/src/execution.rs
Verdict: concerns

# Adversarial review: production vector-validation milestone

## Findings

No critical or significant correctness defect was found in production routing. The evaluator is engine-neutral, run-scoped, and schema-aware; it does not key on source/destination/format names. Preview and run share it. Residual-present execution restores its effective nullability before binding, and schema changes cause replacement rather than accumulation. The scalar oracle is no longer imported by production execution and the architecture test prevents regression.

One significant closure concern remains by explicit scope: high-failure evidence still becomes `Vec<QuarantineCandidate>` then `Vec<QuarantineRecord>`, and `cdf-package` encodes the entire quarantine part into `Vec<u8>` before atomic artifact publication. A 64k batch bounds cardinality, but memory ownership is not exact and the path performs avoidable copies. This does not invalidate the production-kernel milestone, but V2 cannot close until selected evidence is chunked/streamed under the ledger and failure cleanup is proven.

## Verdict

Concerns raised; milestone is safe to commit, V2 remains open.

## Residual risk

The fused benchmark has no row rules and therefore isolates the removal of the old row-sized default-accept vector particularly strongly; V1 separately measures the mixed-rule kernel at 1.73 GiB/s. End-to-end TLC/package evidence must establish the combined macro effect after the evidence writer is streamed.
