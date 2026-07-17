Status: open
Created: 2026-07-11
Updated: 2026-07-16
Parent: .10x/tickets/2026-07-05-implement-cdf-system.md
Depends-On: .10x/tickets/done/2026-07-11-p0-sx1-source-extension-boundary.md, .10x/tickets/done/2026-07-11-p0-dx1-neutral-runtime-crate.md, .10x/specs/foreign-stream-interop.md

# P0 IX1: neutral foreign stream contract

## Scope

Add executor-neutral foreign producer descriptors, incremental outcome/control/terminal stream contract, transfer/copy/lane/memory/cancellation capabilities, conformance mocks, and adapters into the shared source/runtime graph without changing Python/subprocess behavior yet.

## Acceptance criteria

- Contract crate exposes no PyO3/Tokio/process/Wasmtime/DataFusion/CLI types.
- Mock C-data, IPC, and row producers traverse ordinary schema/runtime/package paths incrementally.
- Architecture gates prevent concrete-tier branching and eager collection APIs in generic production runtime.
- Memory/cancellation/copy semantics are structurally declared and conformance-readable.

## Evidence expectations

Dependency graph, API/static checks, mock conformance, compile/test matrix, migration/adaptation notes, and adversarial extension review.

## Explicit exclusions

No Python/subprocess migration, Wasmtime host, or performance claim.

## Blockers

None. SX1 and DX1 are done; this ticket is executable.

## References

- `.10x/decisions/neutral-foreign-stream-boundary.md`
- `.10x/specs/foreign-stream-interop.md`
