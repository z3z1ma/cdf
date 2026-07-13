Status: recorded
Created: 2026-07-11
Updated: 2026-07-12
Target: .10x/tickets/done/2026-07-11-p0-dx1-neutral-runtime-crate.md
Verdict: pass

# DX1 neutral runtime architecture review

## Target

The `cdf-runtime` extraction and project compatibility adaptation.

## Findings

No critical or significant finding remains within DX1 scope.

The runtime crate has no concrete driver, engine, DataFusion, project, or product reference. Per-row execution types did not cross the boundary; dynamic dispatch remains at driver/session operations. Registry ordering is removed from resolution authority, and typed performance declarations are data rather than scheme branches.

The project compatibility layer still constructs the three built-in drivers and retains production convenience constructors. That is an explicit DX1 exclusion with executable removal owner `.10x/tickets/done/2026-07-11-p0-dx2-driver-owned-adapters-composition.md`; it is not accepted as the final architecture.

The policy bridge uses a neutral destination/key lookup rather than moving the Postgres policy type into the runtime crate. This preserves driver-owned interpretation and prevents a concrete destination config from contaminating the neutral contract.

## Verdict

Pass. The change establishes the correct dependency direction without altering package, receipt, checkpoint, or serialized project behavior. DX2 can now move each adapter to its owning destination crate without redesigning the shared interface.

## Residual risk

The extension law is not yet proven with production composition, generic product surfaces, or a fourth conformance catalog entry. DX2-DX4 already own those proofs and remain required before the destination extension parent closes.
