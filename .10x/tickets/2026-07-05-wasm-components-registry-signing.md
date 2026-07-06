Status: open
Created: 2026-07-05
Updated: 2026-07-06
Parent: .10x/tickets/2026-07-05-implement-firn-system.md
Depends-On: .10x/tickets/done/2026-07-05-kernel-core-types.md, .10x/tickets/done/2026-07-05-http-toolkit.md, .10x/tickets/2026-07-05-conformance-chaos-golden.md

# Implement WASM Components, registry gate, and signing

## Scope

Implement `firn-wasm` post-MVP tier: WASI 0.3 WIT world, Wasmtime host, host-mediated HTTP/secrets/logs, Arrow IPC stream ingestion, conformance execution against components, package/connector signature support, and registry-admission hooks. Owns `crates/firn-wasm/**`, WIT specs, and signing modules.

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

## Blockers

None.
