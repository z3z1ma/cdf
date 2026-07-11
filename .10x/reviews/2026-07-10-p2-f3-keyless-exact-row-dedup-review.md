Status: recorded
Created: 2026-07-10
Updated: 2026-07-10
Target: .10x/tickets/done/2026-07-10-p2-ws-f3-keyless-exact-row-dedup.md
Verdict: pass

# P2 F3 keyless exact-row dedup review

## Findings

- Architecture: pass. Intent is kernel resource semantics; execution is a serialized contract verdict; destinations receive ordinary deduplicated segments and contain no feature-specific branch.
- Correctness: pass. Identity uses Arrow's typed row encoding rather than display strings, preventing null/string ambiguity and covering nested values. Package order is deterministic and evidence names retained ordinals.
- Significant, resolved: initially reusing the keyed `Dedup` verdict would have enabled arbitrary contract dedup on append/replace. A distinct `ExactRowDedup` verdict now bypasses the merge gate only for the explicit resource semantic, preserving existing behavior.
- Compatibility: pass. The descriptor field has a serde default, existing constructors explicitly select none, and the validation binding is idempotent because planner binding may follow command-level compilation.

## Verdict

Pass. No unresolved critical, high, or significant findings remain.

## Residual risk

Package-scale identity memory is an explicit P3 performance concern, not a semantic or P2 correctness gap.
