Status: open
Created: 2026-07-11
Updated: 2026-07-18
Parent: .10x/tickets/2026-07-10-p3-ws-h-interop-boundaries.md
Depends-On: .10x/tickets/done/2026-07-11-p3-h1-interop-measurement-copy-proof.md, .10x/tickets/2026-07-08-wasm-wit-interface-foundation.md

# P3 H4: prospective WASM stream cost and interface model

## Scope

Review/validate the WIT foundation against the neutral foreign-stream contract and record a versioned prospective cost model for arbitrary-chunk IPC, startup/compile/instantiate, host calls, memory copies, interruption/fuel, sandbox memory, and capability mediation without implementing a host.

## Acceptance criteria

- WIT can express descriptor/plan/errors, incremental IPC, cancellation, typed control/state, and host-mediated capabilities or names exact required revisions.
- Every cost cell cites named evidence/prototype or remains explicitly unknown.
- No executable/native-equivalent WASM claim appears.
- Later Wasmtime work has concrete benchmark/conformance criteria and no second runtime API.

## Evidence expectations

WIT validation/review, cost worksheet with provenance/limits, reference/prototype bias labels, and adversarial sandbox/performance review.

## Explicit exclusions

No Wasmtime dependency/host, guest SDK, registry, signing, or throughput acceptance claim.

## Blockers

Blocked on WIT foundation. H1 is done.

## References

- `.10x/specs/foreign-stream-interop.md`
- `.10x/tickets/2026-07-05-wasm-components-registry-signing.md`
