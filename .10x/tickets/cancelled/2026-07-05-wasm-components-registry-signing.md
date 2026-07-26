Status: cancelled
Created: 2026-07-05
Updated: 2026-07-25
Parent: .10x/tickets/cancelled/2026-07-05-implement-cdf-system.md
Depends-On: .10x/tickets/done/2026-07-05-kernel-core-types.md, .10x/tickets/done/2026-07-05-http-toolkit.md, .10x/tickets/done/2026-07-05-conformance-chaos-golden.md

# Implement WASM Components, registry gate, and signing

## Scope

Implement `cdf-wasm` post-MVP tier: WASI 0.3 WIT world, Wasmtime host, host-mediated HTTP/secrets/logs, Arrow IPC stream ingestion, conformance execution against components, package/connector signature support, and registry-admission hooks. Owns `crates/cdf-wasm/**`, WIT specs, and signing modules.

## Acceptance criteria

- WIT targets WASI 0.3 and exports `describe`, `negotiate`, and async `open(partition) -> stream<u8>`.
- WASM guests have no ambient filesystem or direct sockets unless explicitly granted.
- Host-mediated HTTP enforces rate limits, redaction, and egress allowlists.
- Component resources pass the same conformance suite as native resources.
- Package signature slot can be populated and verified without changing package layout.

## Evidence expectations

Record Wasmtime integration tests, WIT compatibility checks, sandbox denial tests, conformance-on-component output, and signing verification tests.

## Explicit exclusions

WASI 1.0 freeze changes require a later decision if they alter the interface.

## Progress and notes

- 2026-07-05: Opened from book and specs.
- 2026-07-08: Split child `.10x/tickets/cancelled/2026-07-08-wasm-wit-interface-foundation.md` for the first reviewable WIT package/world artifact. Host execution, sandbox denial, registry admission, signing, and conformance-on-component remain parent scope.

## Blockers

None.

## Cancellation

Cancelled as an active ticket on 2026-07-25. The ticket combines versioned WIT, a Wasmtime host,
capability brokers, sandboxing, conformance, registry admission, and signing while the underlying
foreign-stream state machine and recursive-value projection remain unresolved. Existing research
and review are preserved. A future WASM program starts from the activation conditions in
`.10x/knowledge/active-backlog-and-future-roadmap.md`, not from this monolith.
