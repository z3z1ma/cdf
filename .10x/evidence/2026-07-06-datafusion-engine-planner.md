Status: recorded
Created: 2026-07-06
Updated: 2026-07-06
Relates-To: .10x/tickets/done/2026-07-05-datafusion-engine-planner.md, .10x/specs/architecture-layering-runtime.md, .10x/specs/resource-authoring-planning-batches.md, .10x/specs/types-contracts-normalization.md, .10x/specs/package-lifecycle-determinism.md

# DataFusion engine planner evidence

## What was observed

`firn-engine` now contains an MVP planner/execution surface for mock resources:

- Tier A resources use engine-side residual filtering, projection, and limit handling and can write package-ready Arrow IPC segments through `firn-package`.
- Tier B resources negotiate projection/filter/limit/partition information without opening the resource stream and preserve per-predicate `Exact`, `Inexact`, and `Unsupported` fidelity.
- Inexact and unsupported predicates remain residual engine predicates; exact predicates are not reapplied.
- Illegal unbounded live plans are rejected at plan time. Bounded and unbounded drain plans are accepted.
- Explain data includes pushed, inexact, unsupported, partition, estimate, boundedness, operator-chain, and delivery-guarantee details.
- Operator-chain records include `DataFusionTableProvider`, `DataFusionScanExec`, `SchemaFingerprintExec`, `ContractExec`, `NormalizeExec`, `ProfileExec`, `LineageExec`, and `PackageSink`.
- Execution reuses `firn-contract::ValidationProgram` and `firn-package::PackageBuilder`; no cross-crate API additions were made.

## Procedure

Commands run from `/Users/alexanderbut/code_projects/personal/firn`:

```text
cargo fmt -p firn-engine
```

Result: passed with exit code 0.

```text
cargo test -p firn-engine --locked --no-fail-fast
```

Result: passed with exit code 0. The final run reported 5 unit tests passed and 0 failed, plus 0 doctests.

```text
cargo clippy -p firn-engine --all-targets --locked -- -D warnings
```

Result: passed with exit code 0.

```text
git diff --check
```

Result: passed with exit code 0 before evidence/ticket record edits.

## What this supports or challenges

This supports the ticket acceptance criteria for mock resource planning/execution, package output, pushdown-fidelity negotiation, residual predicate handling, unbounded rejection, explain fields, and contract/package operator-chain integration.

It also supports the architecture-layering constraint that `firn-engine` composes kernel, contract, and package semantics rather than redefining them.

## Limits

The MVP does not implement concrete HTTP, Python, subprocess, destination drivers, or external resource implementations.

DataFusion 54 currently depends on Arrow 58.3 while the lower Firn crates use Arrow 59. The MVP keeps DataFusion types isolated to planner-facing fidelity mapping and operator-chain identity, and uses Arrow 59 batches for Firn execution/package output so lower-layer public APIs remain unchanged.
