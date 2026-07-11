Status: active
Created: 2026-07-11
Updated: 2026-07-11

# Native format codec runtime

## Purpose and scope

This specification governs byte-source/transform/format driver contracts, registries, configuration, detection, discovery/decode parity, decode units, parse outcomes, dependency isolation, and format conformance.

## Byte source

A `ByteSource` MUST expose stable source content identity and capabilities for sequential chunks, exact ranges, known/unknown length, reopenability, seekability, and useful range concurrency. Returned chunks MUST be accounted and bounded. Short/ignored ranges, identity changes, and retry exhaustion fail with transport evidence; codecs MUST NOT silently accept a different object generation.

Local, HTTP, and object-store transports MUST satisfy the same contract. Format drivers MUST NOT receive credentials, secret values, URLs requiring policy decisions, raw filesystem handles, or concrete transport clients. Spooling is a planner/runtime adapter selected when driver and source capabilities require it, with disk budget and identity verification.

## Transform pipeline

Byte transforms MUST declare signatures/extensions, streaming/random-access effect, concatenated-member behavior, maximum window/working set, expansion safeguards, checksum behavior, and splittability. Auto-detection uses extension plus magic; explicit configuration wins only when magic is not contradictory.

Every decompressor MUST stream under the ledger and enforce configured expanded-byte and compression-ratio ceilings. A transform cannot buffer the complete decoded object. Archive containers enumerate members as logical child partitions with member identity and safety limits and are governed separately from byte compression.

## Format registry and configuration

Format ids are validated stable strings. Registration rejects duplicate ids/aliases and conflicting strong magic signatures deterministically. A driver supplies canonical option schema/defaults; unknown or invalid options fail before source contact where possible.

Project schema generation MUST include installed first-party formats without making the declarative parser a closed Rust enum. Lock/plan artifacts pin format id, driver semantic version, canonical options, detection signals, and decode-unit policy. A version change that can alter rows, schema, order, parse outcomes, or positions is a plan/package semantic change surfaced by diff.

## Detection and discovery

Explicit format, extension, MIME, and bounded prefix/suffix magic observations are separate evidence. Strong explicit/magic conflict fails with both signals and fixes. Ambiguous auto-detection lists candidates and requires `format`. Text detection MUST NOT guess between ambiguous delimited/fixed-width/JSON-like inputs without bounded validation.

The same driver MUST own bounded schema discovery and execution physical decoding. Discovery may sample according to the active sampling contract; runtime always validates all decode units. Driver output includes physical Arrow schema/metadata and exact observation identity. Shared reconciliation compiles/executes the declared/pinned constraint.

## Decode units and output

Drivers MAY plan deterministic units inside a logical file only from immutable metadata/ranges. Each unit records file identity, unit kind/id/ordinal, byte/row-group/block range where exact, schema observation, projection/predicate plan, and working-set estimate. Unit order and file completion are plan semantics; scheduler order is not.

Decoders yield accounted physical Arrow outcome envelopes incrementally. Each outcome carries exact file/unit/local sequence and source-position authority plus parse quarantine/residual facts. A fatal window error MUST publish no accepted rows from that window. Row-local recovery MUST identify the record/byte/field sufficiently for deterministic quarantine and cannot silently substitute null/default values.

Projection and predicate pushdown MUST declare exact/inexact/unsupported fidelity. The shared engine re-applies inexact predicates. Decoder batch sizes are internal microbatches and cannot determine canonical package segmentation.

## Dependency and extension law

Neutral contracts live below codecs. Each codec crate MUST depend only on the parser/transform libraries it needs plus neutral CDF contracts. No codec may import declarative project, CLI, concrete transport, destination, or sibling codec implementations.

Adding a codec MUST require no edits to generic declarative compilation, file runtime, discovery dispatcher, preview/run orchestration, or conformance assertions. Static tests enforce those boundaries. The standard product registry and generated authoring catalog are the only first-party composition edits.

## Conformance

Shared conformance MUST cover explicit/auto detection, conflicts/ambiguity, bounded discovery, preview/run physical parity, declared/pinned reconciliation, local/HTTP/object-store sources, sequential/ranged/spooled modes, compression composition, projection/predicate fidelity, malformed/truncated input, no partial fatal window, row quarantine, cancellation, memory/working-set declaration, jobs invariance, identity change, and high-unit-count metadata bounds.

Every codec adds format-specific golden and fuzz corpora, reference-decoder comparisons, roofline benchmarks, and dependency review. A mock external codec must compile/register and pass generic laws without editing shared runtime/compiler code.

## Explicit exclusions

This spec does not define a dynamic ABI, table formats such as Iceberg/Delta, source credential policy, distributed unit scheduling, archive member semantics, or the exact native format catalog.
