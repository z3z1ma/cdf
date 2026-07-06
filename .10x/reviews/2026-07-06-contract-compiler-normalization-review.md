Status: recorded
Created: 2026-07-06
Updated: 2026-07-06
Target: .10x/tickets/done/2026-07-05-contract-compiler-normalization.md
Verdict: pass

# Contract compiler and normalization review

## Target

Review of `crates/firn-contract/**` against `.10x/specs/types-contracts-normalization.md` and `.10x/tickets/done/2026-07-05-contract-compiler-normalization.md`.

## Findings

- Significant, fixed before closure: exact duplicate source names previously bypassed the collision branch because the previous source string matched. The parent patch made any duplicate normalized output name a hard error and added a regression assertion to `normalizer_preserves_source_names_and_rejects_collisions`.

## Verdict

Pass. The implementation covers serializable validation programs, total row disposition lattice, trust preset expansion, decimal/timestamp fidelity guards, `namecase-v1`, nested/variant decisions, transform descriptions, promotion/demotion events, type-mapping fidelity checks, and PII redaction decisions. The fixed duplicate-name case closes the only parent-found blocker.

## Residual risk

This is a compiler/model layer only. DataFusion execution, package materialization of quarantine values, and observed-value hashing remain outside this ticket and are owned by later engine/package work.
