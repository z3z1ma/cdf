Status: open
Created: 2026-07-11
Updated: 2026-07-11
Parent: .10x/tickets/2026-07-05-implement-cdf-system.md
Depends-On: .10x/tickets/done/2026-07-11-p0-dx1-neutral-runtime-crate.md, .10x/tickets/done/2026-07-11-p3-a2-unified-memory-ledger.md, .10x/tickets/done/2026-07-11-p3-a4-injected-execution-host.md, .10x/specs/native-format-codec-runtime.md

# P0 FX1: native format extension and byte-source boundary

## Scope

Add neutral byte-source/transform/format registry contracts, migrate declarative format configuration from closed enums to registry-validated ids/options, split first-party codec build domains, and prove one mock external codec can discover/plan/preview/run through local and remote byte sources without generic orchestration edits. Preserve required behavior while deleting superseded dispatch and compatibility surfaces; decoder optimizations and new formats remain WS-B children.

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

## Progress and notes

- 2026-07-11: Landed the neutral runtime foundation: immutable `ByteSource` identity/capabilities with accounted sequential and exact-range reads; `FormatDriver` discovery/unit-planning/physical-decode contracts; streaming `ByteTransformDriver`; accounted physical Arrow outcomes; and deterministic format/transform registries with pre-mutation id/alias/extension/strong-magic collision checks. Descriptors pin semantic version, options, detection, pushdown fidelity, unit policy, memory, random-access, checksum, member, and expansion claims. No concrete transport, executor, parser, project, CLI, source, or destination type crosses the boundary. Production first-party/declarative migration remains open; the old dispatch is not being retained as a final compatibility surface. Evidence: `.10x/evidence/2026-07-11-p0-fx1-neutral-format-contracts.md`.
- 2026-07-11: Extracted the first parser-local implementation into `cdf-format-parquet`. It uses only the neutral runtime contracts; performs footer discovery, row-group unit planning, projection, bounded-concurrency async range reads, and incremental Arrow decode; and retains source ledger leases through Parquet's owner-backed `Bytes` lifetime. A real Parquet fixture passes detect/discover/plan/decode through a mock external byte source with zero residual memory. Production composition and deletion of the monolithic path remain open. Evidence: `.10x/evidence/2026-07-11-p0-fx1-parquet-driver-extraction.md`.
- 2026-07-11: `cdf-source-files::LocalByteSource` now supplies the first real neutral provider with accounted zero-copy chunks/ranges, cancellation, and strong Unix generation reattestation. Format drivers can consume local bytes without receiving paths or file handles. Production registry selection remains open. Evidence: `.10x/evidence/2026-07-11-p3-g1-local-accounted-byte-source.md`.
- 2026-07-11: Moved physical-to-effective Arrow materialization into `cdf-contract` and made engine preview/run execute the typed per-observation plan when a codec emits an unadorned physical batch. Projection, widening, missing nullable fields, and provenance now have one format-neutral implementation; source metadata injection remains rejected. Accounted ownership for cast allocations and production registry routing remain open. Evidence: `.10x/evidence/2026-07-11-p0-fx1-shared-schema-materialization.md`.
- 2026-07-11: Added one kernel-neutral `PayloadRetention` primitive and moved native physical-batch leases into it when entering the kernel stream. The same primitive replaced the destination-only retention type, so source and commit boundaries share one lifetime mechanism with no `cdf-memory` dependency in the kernel and no compatibility alias. Evidence: `.10x/evidence/2026-07-11-p0-fx1-accounted-batch-retention.md`.
- 2026-07-11: Production local and remote-spooled Parquet now resolve through the CLI-composed `FormatRegistry` and stream from the injected I/O scope through the generic registered-format path. Dependency-free file runtime constructors and compiler-owned file execution functions were deleted. CLI local/HTTP runs pass. TLC medians regressed from about 1.53/1.62 wall/CPU seconds to 1.63/1.80, so B2 remains explicitly responsible for eliminating local neutral-range overhead; unregistered format fallback also remains before FX1 closure. Evidence: `.10x/evidence/2026-07-11-p0-fx1-production-parquet-registry-stream.md`.
- 2026-07-11: A second production format, Arrow IPC file framing, now uses the same registry-selected local/remote-spool execution path without a format-specific execution branch. Its parser dependency is isolated in `cdf-format-arrow-ipc`; the former local IPC reader and remote hard rejection were deleted from `cdf-source-files`. This proves the generic driver seam across two binary formats, but FX1 remains open because schema attestation and CSV/JSON/NDJSON still contain closed-enum/fallback dispatch. Evidence: `.10x/evidence/2026-07-11-p3-b3-native-arrow-ipc-file-driver.md`.
- 2026-07-12: Hardened the neutral byte-transform boundary so implementations can satisfy the ledger rather than manufacture unaccounted output. `ByteTransformRequest` binds allocation owner, output chunk, expanded bytes, ratio, planned input size, and cancellation; the former signature was deleted. First-party transform implementations and registry-driven file composition remain open. Evidence: `.10x/evidence/2026-07-12-p3-b1-transform-allocation-authority.md`.
