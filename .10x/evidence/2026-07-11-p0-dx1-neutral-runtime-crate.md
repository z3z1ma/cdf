Status: recorded
Created: 2026-07-11
Updated: 2026-07-11
Relates-To: .10x/tickets/done/2026-07-11-p0-dx1-neutral-runtime-crate.md, .10x/tickets/2026-07-11-p0-destination-extension-boundary.md

# DX1 neutral runtime extraction evidence

## What was observed

`cdf-runtime` now owns the engine-neutral destination driver, registry, resolution context, runtime, prepared-commit, planning, inspection, capability, and resolved-runtime contracts. Its normal dependency graph contains kernel, contract, HTTP secret-provider, package, Arrow schema, and serde only; it contains no `cdf-project`, `cdf-engine`, DataFusion, DuckDB, or `cdf-dest-*` edge.

The registry rejects empty, malformed, and duplicate schemes deterministically; scheme lookup is case-insensitive; its public registered-scheme view is stable across registration order. A mock external driver resolves and provides no-mutation typed inspection with ingress mode, writer model, segment/byte concurrency, bulk-path evidence, sheet hash, and typed health probes.

`cdf-project` compiles and runs through compatibility re-exports of the extracted contracts. Local compatibility wrappers remain only for the first-party production adapters and convenience constructors; DX2 owns moving those adapters into destination crates and removing the shared built-in registration list.

## Procedure

- `cargo tree -p cdf-runtime --edges normal --offline` showed no upward or concrete destination dependency.
- `cargo test -p cdf-runtime -p cdf-project --lib --offline` passed 4 runtime tests and 171 project tests, including generic mock replay, destination planning, package/receipt/checkpoint crash paths, multi-file manifests, and live local Postgres paths.
- `cargo fmt --all -- --check` passed.
- Existing project artifact/hash and package lifecycle tests ran unchanged and passed through the extracted types.

## What this supports

This supports DX1 acceptance and unblocks DX2. P3 staged ingress and bulk-path declarations now have a neutral serialized vocabulary that can be consumed without destination-name matching or an engine dependency.

## Limits

First-party adapters still live under `cdf-project` and the project crate still depends on concrete destinations. This is intentionally the DX1 compatibility tranche, not the destination-boundary program exit; DX2-DX4 own adapter migration, product injection, generic inspection surfaces, conformance enrollment, and build-graph proof.
