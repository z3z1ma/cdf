Status: done
Created: 2026-07-11
Updated: 2026-07-11
Parent: .10x/tickets/done/2026-07-11-p0-destination-extension-boundary.md
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

None. Complete.

## Progress and notes

- 2026-07-11: Added dependency-neutral `cdf-runtime` authority for driver registration/resolution, typed no-mutation inspection, prepared commits, runtime planning, policy/secret context, and staged/finalized ingress plus concurrency/byte capability declarations.
- 2026-07-11: Adapted `cdf-project` through compatibility re-exports and local adapter wrappers. The complete 171-test project library suite and four runtime boundary tests passed; package/receipt/checkpoint behavior remained stable.
- 2026-07-11: Evidence is `.10x/evidence/2026-07-11-p0-dx1-neutral-runtime-crate.md`; architecture review `.10x/reviews/2026-07-11-p0-dx1-neutral-runtime-review.md` passed. DX2 owns removal of compatibility adapters and built-in project composition.
