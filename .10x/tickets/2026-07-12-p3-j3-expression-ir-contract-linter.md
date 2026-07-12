Status: open
Created: 2026-07-12
Updated: 2026-07-12
Parent: .10x/tickets/2026-07-12-p3-ws-j-datafusion-currency-bridges.md
Depends-On: .10x/specs/datafusion-currency-bridges.md, .10x/tickets/done/2026-07-11-p3-v2-validation-graph-integration.md

# P3 J3: expression IR and contract linter

## Scope

Define a stable CDF expression serialization that round-trips through the pinned DataFusion `Expr`; migrate declarative derive/filter/contract rules; use DataFusion simplification, function resolution, constant folding, and interval reasoning at plan time; lower recorded identity-bearing expressions exclusively to native fused kernels; permit `PhysicalExpr` only for nonidentity analysis/query work; export explicit-fidelity Substrait views.

## Acceptance criteria

- Derive/filter/contract use one semantic expression authority and versioned serialization.
- Optimized expression, functions/versions, fidelity, residuals, and lints are recorded before execution.
- Replay never silently re-optimizes a recorded expression.
- Native fused lowering is the only identity path; unsupported native functions fail planning, while nonidentity `PhysicalExpr` use has differential Arrow/null/error tests.
- Linter detects provably unsatisfiable ranges, always-true rules, and cursor-subsumed filters without inventing conclusions under uncertainty.
- Kernel/contract public APIs expose no DataFusion Rust types.

## Evidence expectations

Parser/serialization goldens, native/DataFusion differential property tests, optimizer-version replay fixtures, function catalog coverage, Substrait fidelity cases, throughput comparison, and review.

## Explicit exclusions

No arbitrary SQL execution inside contracts and no runtime optimizer authority.

## Blockers

V2 established the fused validation graph seam; no remaining blocker.
