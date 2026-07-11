Status: recorded
Created: 2026-07-11
Updated: 2026-07-11
Target: .10x/tickets/done/2026-07-11-p3-a1-staged-ingress-final-binding.md
Verdict: pass

# P3 A1 staged-ingress architecture review

## Target

The neutral staged-ingress and verified final package-binding API, capability declaration, compatibility defaults, and conformance tests.

## Findings

### Resolved — significant: reattachment authority was initially under-bound

The first implementation keyed recovery by `LoadAttemptId` and segment identities but did not make the attempt's destination, target, disposition, schema, and plan authority part of its recovery snapshot. A driver could therefore accidentally reattach the same opaque id under different authority. The implementation now uses immutable `StagingAttemptBinding` in requests and snapshots, validates it on reattachment and final binding, and tests the mismatch path.

### Resolved — significant: provisional commit authority must be unrepresentable

Staging request and acknowledgement types contain no package hash, package token, receipt, or committed segment acknowledgement. `VerifiedFinalBinding` fields are private outside the crate and its public constructor requires a verified package. Tests inspect serialized shapes and final receipt timing.

### No concern: destination specialization

The contract is destination-neutral and capability-driven. Existing destinations only add an honest finalized-only declaration. No source/destination name branch or concrete adapter moved into the runtime crate.

## Verdict

Pass. The implementation preserves package identity, receipt/checkpoint authority, deterministic segment order, and the destination extension boundary while enabling later streaming overlap.

## Residual risk

Real transactional and object-store staging semantics are not claimed here. They require destination-specific conformance and chaos evidence in their owning WS-D/A5 tickets before any destination may advertise staged ingress.
