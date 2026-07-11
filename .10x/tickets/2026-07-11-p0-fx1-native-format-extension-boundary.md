Status: open
Created: 2026-07-11
Updated: 2026-07-11
Parent: .10x/tickets/2026-07-05-implement-cdf-system.md
Depends-On: .10x/tickets/done/2026-07-11-p0-dx1-neutral-runtime-crate.md, .10x/tickets/done/2026-07-11-p3-a2-unified-memory-ledger.md, .10x/tickets/done/2026-07-11-p3-a4-injected-execution-host.md, .10x/specs/native-format-codec-runtime.md

# P0 FX1: native format extension and byte-source boundary

## Scope

Add neutral byte-source/transform/format registry contracts, migrate declarative format configuration from closed enums to registry-validated ids/options, split first-party codec build domains, and prove one mock external codec can discover/plan/preview/run through local and remote byte sources without generic orchestration edits. Preserve existing codec behavior through compatibility adapters; decoder optimizations and new formats remain WS-B children.

## Acceptance criteria

- Generic declarative/file runtime contains no first-party format match tree; format selection/discovery/decode route through a registry.
- Byte-source/codec contracts expose no Tokio/object-store/HTTP/filesystem/parser types and use accounted envelopes.
- Physical decode and shared schema reconciliation are separate; discovery/preview/run use one driver interpretation.
- Logical-file decode units preserve whole-file `FileManifest` completion and deterministic order.
- Existing Parquet/CSV/JSON/NDJSON/Arrow IPC behavior passes compatibility conformance.
- Parser dependencies are codec-local; changing/building one codec does not force unrelated codec implementation crates.
- A mock codec and transform register with only composition/catalog/fixture edits and pass shared laws.

## Evidence expectations

Cargo graph/rebuild evidence, architecture static tests, registry/config schema migration, mock codec/transform, local/remote/spool conformance, artifact/golden compatibility, malformed/cancellation/memory tests, and adversarial extension review.

## Explicit exclusions

No new parser dependency, new native format, optimized decoder, dynamic plugin ABI, archive container, or distributed decode.

## Blockers

Depends on neutral runtime, memory, and execution-host contracts. It must land before WS-B adds more codecs.

## References

- `.10x/decisions/native-format-driver-and-byte-source-boundary.md`
- `.10x/research/2026-07-11-format-extension-streaming-audit.md`
- `.10x/specs/native-format-codec-runtime.md`
- `.10x/knowledge/source-destination-extension-invariant.md`
