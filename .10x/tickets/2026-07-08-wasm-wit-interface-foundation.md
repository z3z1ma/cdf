Status: open
Created: 2026-07-08
Updated: 2026-07-08
Parent: .10x/tickets/2026-07-05-wasm-components-registry-signing.md
Depends-On: .10x/specs/resource-authoring-planning-batches.md, .10x/specs/conformance-governance-roadmap.md

# Implement WASM WIT interface foundation

## Scope

Add the first reviewable Tier-3 WASM component interface artifact under `crates/cdf-wasm/**`.

Owns:

- The CDF resource WIT package/world file(s).
- Minimal crate documentation and tests that make the WIT file discoverable from `cdf-wasm`.
- Any small validation script or test fixture needed to prove the WIT file contains the ratified package, imports, exports, and async stream shape.

## Acceptance criteria

- The WIT package is named `cdf:resource@0.1.0`.
- The exported world is named `resource`.
- The world imports host-mediated `cdf:host/http`, `cdf:host/secrets`, and `cdf:host/log` interfaces.
- The world exports `describe`, `negotiate`, and `open`.
- `open` is declared as an async function taking a partition value and returning `stream<u8>` of Arrow IPC bytes.
- The WIT artifact models enough descriptor, scan request, scan plan, partition, and error/result structure that the interface is reviewable and not a placeholder.
- The implementation does not add a Wasmtime host, sandbox execution, registry admission, signing, component SDK, or conformance-on-component runtime.

## Evidence expectations

Run `cargo fmt --all --check`, `cargo test -p cdf-wasm --locked`, `cargo clippy -p cdf-wasm --all-targets --locked -- -D warnings`, source-only Gitleaks over `crates/cdf-wasm` and changed `.10x` records, direct unsafe/FFI scan over `crates/cdf-wasm`, and a WIT syntax check if a local `wasm-tools` or equivalent validator is available without weakening the dependency graph. If no WIT validator is locally available, record that limit and compensate with focused text-shape tests.

## Explicit exclusions

No Wasmtime host, no component execution, no sandbox-denial tests, no host HTTP/secrets/log implementation, no registry admission, no signing implementation, no guest SDK, no runtime integration, no Cargo dependency additions unless strictly needed for local validation.

## Blockers

None. The WIT shape is ratified by `VISION.md` §9.5 / D-26 and `.10x/specs/resource-authoring-planning-batches.md`.

## Progress and notes

- 2026-07-08: Split from `.10x/tickets/2026-07-05-wasm-components-registry-signing.md` because the full WASM/registry/signing ticket is too broad to execute as one slice. This child only makes the WIT interface artifact concrete and reviewable.
