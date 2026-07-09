Status: active
Created: 2026-07-08
Updated: 2026-07-08

# Data onramp file source transport and manifest policy

## Context

The P2 directive identifies the current file source as a one-local-file slice: run rejects multi-file globs, preview samples one match, remote transports are absent, compression is manual, and `FileManifest` exists without being the default incrementality mechanism for file resources.

`VISION.md` sections 8.6 and 13.3 make files, prefixes, row groups, and typed `FileManifest` positions first-class. Appendix C already reserves the `object_store` integration role.

## Decision

File globs are partitions. A glob matching N files plans N file partitions in deterministic order; each partition has file-scoped source position evidence and can be retried independently. The old single-file run rejection is removed by P2 implementation work. Large-N coalescing is allowed only as a later bounded batching decision; until a threshold is ratified in a child ticket, implementation should plan one partition per file for modest-N conformance and product cases.

`FileManifest` incrementality is the default for file resources. The first successful run records a manifest for the resource scope. Subsequent plans compare the current listing against the committed manifest and plan only new or changed entries. Identity uses the strongest available transport evidence: checksum when available or computed as part of required reading, stable ETag where the transport declares it meaningful, plus size and modification time when provided. A no-change file run is a fast no-op with an explicit report.

Compression is transparent for gzip and zstd. Detection uses extension plus magic-byte confirmation, streaming decode is required, and `compression = "none|gzip|zstd|auto"` overrides auto-detection. Zip archives are deferred because archive member identity and member-glob semantics need a separate decision before implementation.

Format auto-detection uses extension with magic-byte confirmation. Explicit `format` wins. Conflicts between extension, magic, and explicit declaration are plan-time errors that name both signals.

Remote file access uses one facade over local files, S3, GCS, Azure Blob, and HTTP(S). The object-store-backed facade owns listing, ranged reads, streaming reads, and seek/spool decisions. Credentials are `secret://` references resolved through the existing provider chain and egress allowlists apply before network access. HTTP(S) supports single public files and template/range enumeration for index-less public datasets; arbitrary HTTP directory listing is not assumed.

Non-seekable or remote formats may spool into the package working area under configured memory/disk budgets. Spooling is an implementation detail of the file facade and must not bypass plan, package, receipt, checkpoint, redaction, or egress policy.

Per-file schema variance is governed by the resource contract. `evolve` unions compatible/widening differences across files; `freeze` quarantines nonconforming files or rows according to the contract and source-decode policy. Mixed schemas never crash with an unclassified internal error.

## Alternatives considered

Continue rejecting multi-file run.

- Rejected because it contradicts `VISION.md` partitioning, `FileManifest`, and the P2 S2/S8 golden paths.

Treat HTTP(S) as local download only.

- Rejected because public-data first use cases require ranged Parquet footer probes and evidence-preserving streaming without full manual preprocessing.

Infer archive member behavior for zip now.

- Rejected because archive-of-members has identity and glob semantics distinct from gzip/zstd stream compression.

## Consequences

File source execution, preview, schema discovery, state, and conformance must share one file-resolution facade.

Transport work can start with local and HTTP(S) while preserving the same trait/facade for S3, GCS, and Azure.

Conformance must prove manifest incrementality, no-op reruns, compression, remote/ranged discovery, preview/run parity, and contract-governed schema variance.
