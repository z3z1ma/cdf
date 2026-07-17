Status: recorded
Created: 2026-07-11
Updated: 2026-07-11
Relates-To: .10x/tickets/done/2026-07-11-p0-sx1-source-extension-boundary.md

# Neutral source registry foundation

## What was observed

`cdf-runtime` now owns object-safe source-driver registration, deterministic kind and URI-scheme indexes, a serializable compiled source plan, injected secret/execution resolution context, and scheduler-facing capabilities for working-set bounds, concurrency, executor class, retry, attestation, rate/quota authority, ordering, and boundedness.

The registry rejects duplicate driver ids, kinds, and schemes. Compilation verifies that a driver emitted its own registered authority; serialized plans bind driver version and option-schema identity plus canonical hashes of redacted options and the physical plan; resolution rejects missing or changed driver authority.

## Procedure

- `cargo test -p cdf-runtime source_registry_compiles_hashes_and_resolves_mock_without_order_authority -- --nocapture` — passed.
- `cargo clippy -p cdf-runtime --all-targets -- -D warnings` — passed.

## What this supports

A source implementation can compile and resolve through neutral runtime contracts without adding source identifiers to the scheduler. The plan carries enough declared execution behavior for later memory and concurrency admission.

## Limits

This evidence covers the neutral boundary and mock driver only. File, REST, and Postgres still require migration through the registry; generic match-tree deletion, schema-catalog composition, dependency isolation, and product parity remain acceptance work on SX1.
