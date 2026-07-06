Status: open
Created: 2026-07-05
Updated: 2026-07-06
Parent: .10x/tickets/2026-07-05-implement-firn-system.md
Depends-On: .10x/tickets/done/2026-07-05-kernel-core-types.md, .10x/tickets/done/2026-07-05-contract-compiler-normalization.md, .10x/tickets/done/2026-07-05-package-builder-reader.md

# Implement DataFusion engine, planner, operators, and explain

## Scope

Implement `firn-engine` adapters from resources to DataFusion table providers, scan planning, pushdown negotiation, boundedness checks, physical operator chain, execution orchestration into packages, guarantee-line rendering hooks, and explain output. Owns `crates/firn-engine/**`.

## Acceptance criteria

- Tier A resources run through engine-side projection/filter/limit and still produce package-ready batches.
- Tier B resources negotiate projection/filter/limit/partitioning without I/O and expose per-predicate fidelity.
- `ContractExec`, `NormalizeExec`, `ProfileExec`, and lineage/package sink integration enforce compiled programs without redefining semantics.
- Plans reject illegal unbounded streams.
- `firn explain` data includes pushed, inexact, unsupported, partition, estimate, and guarantee details.

## Evidence expectations

Record engine integration tests with mock resources, explain snapshots, unbounded rejection tests, and package-output tests.

## Explicit exclusions

No concrete HTTP, Python, subprocess, or destination driver implementation.

## Progress and notes

- 2026-07-05: Opened from book and specs.

## Blockers

None.
