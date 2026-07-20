Status: recorded
Created: 2026-07-11
Updated: 2026-07-11
Target: .10x/decisions/destination-runtime-composition-boundary.md, .10x/specs/destination-extension-runtime-contract.md, .10x/tickets/done/2026-07-11-p0-destination-extension-boundary.md
Verdict: pass

# Destination extension boundary shaping review

## Assumptions tested

- Whether another helper inside `cdf-project` could remove the concrete fan-out: it cannot correct the dependency direction because destination crates would need to depend upward on the product crate.
- Whether the kernel should own package-aware runtime planning: rejected because package, secret-resolution, and driver-private planning types do not belong in the kernel.
- Whether implicit auto-registration would reduce extension work safely: rejected because it hides composition, weakens deterministic embedding, and complicates WASM/distributed products.
- Whether object-safe dispatch harms the hot path: dispatch is constrained to driver/session/segment boundaries and is explicitly subject to P3 measurement, never per value or row.

## Findings

No critical or significant shaping defect remains. The new boundary has one clear neutral owner, explicit composition, a testable three-touch extension scenario, driver-owned planning, and data-driven conformance. It avoids speculative dynamic-plugin machinery while preserving an embedding path.

## Verdict

Pass for activation. The executable graph is correctly ordered: neutral extraction, driver migration, generic product surfaces, then conformance/build-graph enforcement.

## Residual risk

The exact Rust type split may expose a cycle involving HTTP/secret context or package planning during DX1. That is an implementation detail within the ratified dependency rule; if no acyclic placement preserves it, DX1 must block and return the concrete cycle to shaping rather than putting destination types back in `cdf-project`.
