Status: active
Created: 2026-07-10
Updated: 2026-07-10

# Destination protocol capabilities use a defaulted aggregate extension seam

## Context

RP3 needs typed, versioned destination correction capabilities in generic planning, conformance, and lock snapshots. `cdf_kernel::DestinationSheet` is a public, externally constructible struct. Adding a field with `serde(default)` preserves serialized input but breaks every downstream Rust struct literal, as reproduced by `cargo semver-checks` and described in `.10x/knowledge/content-addressed-sidecar-publication.md`. Requiring every destination to spell an unsupported value also violates `.10x/knowledge/source-destination-extension-invariant.md`.

## Decision

`DestinationSheet` remains field-for-field and byte-for-byte compatible. `DestinationProtocol` exposes a provided `protocol_capabilities()` method returning a versioned, non-exhaustive `DestinationProtocolCapabilities` aggregate whose default is truthful unsupported behavior. New destinations inherit that default without boilerplate. An adapter overrides the method only when it has evidence for a non-default claim.

`DestinationSheetArtifact` flattens the legacy sheet and adds the aggregate as a defaulted, omitted-when-default serialized slot. Generic consumers obtain it through the provided `sheet_artifact()` method. Legacy sheet serialization and hashes remain exact; non-default capabilities participate in artifact identity.

Lock snapshots store the same typed aggregate on `LockedDestination`. Establishing that durable lock extension seam is a one-time Rust construction break: `LockedDestination` is now non-exhaustive and constructed through `LockedDestination::new(DestinationSheetArtifact)`. The existing `generate_lockfile` API remains available and assigns default capabilities; the artifact-aware API snapshots adapter claims. Parent review selected this one-time pre-1.0 construction migration under the user's P0 extension-architecture invariant because preserving direct struct-literal construction would force repeated field additions for every future capability family. The user did not separately ratify a general exception to compatibility policy.

Generic planning and conformance consume protocol capabilities only. They MUST NOT branch on DuckDB, Postgres, Parquet, or future destination names.

## Alternatives considered

- Add `corrections` directly to `DestinationSheet`: rejected because it breaks every destination and downstream struct literal and makes extension cost proportional to adapter count.
- Encode correction claims in type mappings, metadata strings, or identifier rules: rejected because it is untyped, semantically dishonest, and not independently falsifiable.
- Keep correction capabilities only in an unreferenced sidecar: rejected because lock serialization would not snapshot the authority used by planning.
- Add a correction-only field to the lock without an aggregate seam: rejected because the next capability family would repeat the same public-shape break.

## Consequences

- `cdf-kernel` remains source compatible; its semver check passes all current checks.
- Legacy destination sheets and locks with default capabilities preserve exact serialized bytes and sheet hashes.
- Postgres lock snapshots include its typed provenance-persistence claim; DuckDB, Parquet, and new adapters omit the default aggregate.
- `cdf-project` downstream code that directly constructed `LockedDestination` must migrate once to `LockedDestination::new`; the semver check reports this intentional major construction change.
- Future protocol capability families extend the non-exhaustive aggregate and builders rather than every destination sheet literal.
