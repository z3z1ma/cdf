Status: done
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
- 2026-07-06: Assigned to engine worker after contract compiler and package builder closure. Worker owns `crates/firn-engine/**` and may propose minimal cross-crate API additions only when required by the active specs; leave unrelated dirty `.gitignore` untouched.
- 2026-07-06: Implemented the `firn-engine` MVP planner/execution surface for mock Tier A/Tier B resources, pushdown fidelity negotiation, boundedness rejection, explain data, operator-chain records, and package output. No cross-crate API additions were required. Evidence recorded in `.10x/evidence/2026-07-06-datafusion-engine-planner.md`; closure review recorded in `.10x/reviews/2026-07-06-datafusion-engine-planner-review.md`.
- 2026-07-06: Workspace quality gates for the engine/declarative/formats batch are recorded in `.10x/evidence/2026-07-06-engine-declarative-formats-quality-gates.md`.

## Blockers

None.
