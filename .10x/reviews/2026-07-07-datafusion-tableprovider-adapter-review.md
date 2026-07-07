Status: recorded
Created: 2026-07-07
Updated: 2026-07-07
Target: .10x/tickets/done/2026-07-07-datafusion-tableprovider-adapter.md
Verdict: pass

# DataFusion TableProvider adapter review

## Target

Implementation of `.10x/tickets/done/2026-07-07-datafusion-tableprovider-adapter.md` in `crates/cdf-engine/src/table_provider.rs`, `crates/cdf-engine/src/lib.rs`, and `crates/cdf-engine/src/tests.rs`.

## Findings

No blocking findings.

The adapter keeps DataFusion ownership in `cdf-engine`; `rg` and source inspection show no DataFusion API exposure in `cdf-kernel`. `lib.rs` remains thin with one module declaration and re-export.

The predicate translator is deliberately narrow: it accepts only simple column/literal comparisons and leaves arithmetic or other expression forms unsupported. This matches the ticket's first-slice constraint and avoids stringifying arbitrary DataFusion expressions into CDF predicates.

Limit pushdown is suppressed when the negotiated scan contains any inexact pushed predicate. Tests prove both suppression for inexact filters and preservation for exact filters.

## Residual risk

The tests exercise real DataFusion provider/execution APIs directly, but they do not yet prove end-to-end DataFrame optimizer residual behavior or package execution through DataFusion physical plans. That is acceptable for this ticket because package execution replacement was explicitly excluded until CDF provenance and `BatchHeader` handling are covered.

`cargo-geiger` did not produce usable evidence in this workspace/tool combination: virtual-manifest invocation is unsupported, and package-manifest invocations emitted registry matching noise and were interrupted after they stopped producing useful output. The source unsafe scan, clippy, CodeQL, and absence of unsafe/FFI/raw-pointer source matches cover the changed first-party code for this slice.

## Verdict

Pass. The implementation satisfies the ticket acceptance criteria and should be closed with evidence `.10x/evidence/2026-07-07-datafusion-tableprovider-adapter.md`.
