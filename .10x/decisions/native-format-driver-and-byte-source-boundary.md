Status: active
Created: 2026-07-11
Updated: 2026-07-11

# Native format driver and byte-source boundary

## Context

CDF's current file-format path is a monolith: closed format/compression enums control compiler/runtime matches, complete inputs/batches are collected, remote behavior forks from local, and all parser dependencies share one crate. This cannot support enterprise native coverage, constant memory, or a straightforward “add one format” workflow.

## Decision

`cdf-runtime` defines executor-neutral object-safe contracts for:

- `ByteSource`: immutable content identity plus sequential accounted chunks, exact ranged reads, length/seekability/reopen/concurrency capabilities, and cancellation;
- `ByteTransformDriver`: compression/encoding transforms from one accounted byte stream to another with expansion/working-set declarations;
- `FormatDriver`: stable id/version, aliases/extensions/MIME/magic detection, option-schema validation, bounded discovery, decode-unit planning, projection/predicate capabilities, physical Arrow decoding, parse outcomes, and working-set declarations;
- deterministic registries for transforms and formats.

These contracts expose boxed futures/streams and neutral memory/execution handles, not Tokio, object-store, HTTP, filesystem, or parser-library types. Transport drivers create `ByteSource`; format drivers never resolve credentials, egress, buckets, or URLs.

Tier-0 format configuration uses a string `FormatId` plus a driver-validated options table. The published project JSON Schema enumerates installed first-party ids/options for authoring assistance, but the runtime parser is registry-driven rather than a Rust closed enum. Plans pin driver id/version, canonical options, detection evidence, decode-unit policy, and capabilities.

Explicit format wins. Auto-detection combines extension, MIME, and bounded magic probes; magic confirms binary formats, text heuristics are evidence rather than certainty, ties fail with candidate/fix diagnostics, and a mismatch between explicit format and strong magic fails plan. Discovery and execution call the same driver and physical-schema interpretation.

Drivers emit physical Arrow outcome streams. Decode hints may improve parsing, but declared/pinned schema does not become a second decoder truth: the shared reconciliation/contract graph consumes physical schema and parse outcomes identically for every format. A fatal decode window emits no accepted partial outcome; row-local recoverable parse failures may travel as bounded pre-contract quarantine facts under compiled policy.

A logical file plan may contain ordered `DecodeUnitPlan`s such as Parquet row groups, ORC stripes, Avro blocks, Arrow record batches, or safe delimited byte ranges. Units may run/retry concurrently, but file-level `FileManifest` position becomes complete only when all required units attest. Canonical unit/row order is plan-derived.

Compression and character decoding are composable transforms; archive containers are separate member-partition sources, not compression codecs. Transforms declare whether random access/splitting survives. Planner joins those capabilities rather than hard-coding “compressed Parquet unsupported” branches.

First-party codecs live in dependency-isolated crates (for example `cdf-format-parquet`, `cdf-format-json`, `cdf-format-avro`). A lightweight `cdf-formats` facade may compose selected drivers but MUST NOT make every codec dependency mandatory for embedders. The standard CLI composes the native set once; minimal embeddings select a subset. Adding a format changes its codec crate, the composition catalog, option-schema export, and a conformance fixture—never declarative compiler/file-runtime match trees.

## Alternatives considered

- Keep adding enum variants/match arms: rejected because every format edits shared semantics.
- One universal DataFusion listing-table format layer: rejected because CDF needs parse quarantine, physical provenance, discovery pinning, exact positions, and non-file transports under its own contracts.
- Expose `AsyncRead`/Tokio/object-store types: rejected because extension and embedding contracts would leak host/transport choices.
- Put every parser in `cdf-formats`: rejected because build/rebuild and supply-chain domains grow together.
- Dynamic plugin ABI now: rejected; explicit compile-time registry composition is sufficient and safer.
- Treat row groups/blocks as independent manifest files: rejected because partial success would advance file incrementality incorrectly.

## Consequences

Current enums and eager reader functions become compatibility facades during migration. File runtime loses format matches. Discovery, preview, deep validation, plan, and run gain one driver path. The build graph becomes codec-local. Each native codec must publish capability/performance evidence and pass shared malformed-input, schema, positions, memory, jobs, and detection conformance.
