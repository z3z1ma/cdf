Status: recorded
Created: 2026-07-11
Updated: 2026-07-11
Relates-To: .10x/tickets/2026-07-11-p0-fx1-native-format-extension-boundary.md

# Neutral format/byte-transform contract foundation

## What was observed

`cdf-runtime` now owns executor- and transport-neutral contracts for immutable byte sources, accounted sequential/range reads, format drivers, byte transforms, physical schema discovery, deterministic decode-unit planning, accounted physical Arrow outcomes, and deterministic registries.

The contracts expose no filesystem, HTTP, object-store, Tokio, parser, project, CLI, or destination types. Format descriptors carry stable id/version/options, detection evidence, pushdown fidelity, decode-unit policy, and working-set bounds. Transform descriptors carry streaming/random-access behavior, member/checksum semantics, expansion ceilings, and working-set bounds. Registries reject duplicate ids/aliases and conflicting strong magic before mutation.

## Procedure

- `cargo test -p cdf-runtime format::tests --lib`
- `cargo clippy -p cdf-runtime --all-targets -- -D warnings`
- Inspected `crates/cdf-runtime/src/format.rs` imports and public signatures for forbidden concrete transport/executor/parser types.

Both commands passed on 2026-07-11. Registry conformance exercises alias resolution, duplicate id rejection, strong-magic collision rejection, and incoherent byte-source range claims.

## What this supports or challenges

This establishes the lower contract required to move first-party codecs and transforms out of the closed `cdf-formats`/file-runtime dispatch. It supports adding format implementations without placing parser dependencies or source-specific branches in generic orchestration.

## Limits

This is the neutral foundation, not FX1 closure. First-party drivers, local/remote byte-source adapters, declarative `FormatId` migration, registry composition, and the mock external codec end-to-end law remain owned by the open FX1 ticket.
