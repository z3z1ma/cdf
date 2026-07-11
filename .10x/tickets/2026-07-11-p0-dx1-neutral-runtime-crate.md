Status: open
Created: 2026-07-11
Updated: 2026-07-11
Parent: .10x/tickets/2026-07-11-p0-destination-extension-boundary.md
Depends-On: .10x/specs/destination-extension-runtime-contract.md

# P0 DX1: neutral runtime crate

## Scope

Create `cdf-runtime`, move destination-neutral registry/resolution/inspection/prepared-commit/runtime types and traits from `cdf-project`, preserve serialized/output behavior, and add dependency-cycle/layer tests. Do not migrate production drivers yet; compatibility re-exports may exist only within this tranche and must have a removal owner in DX2.

## Acceptance criteria

- `cdf-runtime` depends on no concrete source/destination/product crate.
- `cdf-runtime` depends on no engine or DataFusion implementation; engine/product composition depends on the neutral runtime contract, never the reverse.
- Existing project runtime compiles through the extracted types without semantic artifact changes.
- Registry duplicate/scheme/order tests and mock driver inspection/resolution tests pass.
- The interface includes typed inspection and streaming/staging capability vocabulary needed by P3 without implementing scheduling.

## Blockers

None.
