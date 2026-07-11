Status: active
Created: 2026-07-11
Updated: 2026-07-11

# Hash-while-write and package durability barriers

## Context

Package construction rereads every segment twice and every other artifact once solely to recover hashes it could know while writing. It syncs trace/directory per event and holds manifest metadata/bytes in memory. Removing work carelessly could weaken the durable-segment staging contract or atomic package publication.

## Decision

`cdf-package` exposes an internal streaming `ArtifactSink` that tees exact emitted bytes through SHA-256 and a byte counter while writing an exclusive temp sibling. Successful finish flushes encoder buffers, fsyncs the temp file according to durability class, atomically renames, performs the required directory barrier, and returns `WrittenArtifact { path, byte_count, sha256, durability }`. Failed finish removes/abandons the temp and returns no receipt.

All package writers—including IPC, canonical JSON, Parquet evidence, schema, trace, and future streamed metadata—register `WrittenArtifact` receipts in an append/spill-backed draft index. Segment entries derive from the writer receipt plus row count/id. No writer-owned file is reopened solely to compute its manifest entry, and finalization does not content-rehash registered receipts.

Durability classes are:

- `segment_publish`: file sync, atomic rename, and containing-directory sync complete before the segment may be exposed to staged ingress or a durable-segment reader;
- `phase_metadata`: file sync/atomic rename complete; directory sync may coalesce at the next named phase barrier while no final manifest references the file;
- `final_manifest`: every identity directory barrier completes first, then manifest file sync, atomic rename, and package-directory sync publish the package;
- `append_log`: bounded trace/receipt appends flush at named semantic barriers; identity trace is fully synced before final manifest, while append-only receipts retain their existing post-commit durability contract.

Directory sync coalescing never crosses a visibility authority: a segment staged as durable has its directory entry synced; a package manifest is never published before all referenced entries are synced. Status/lifecycle atomicity remains. Trace buffering is bounded/accounted and cannot reorder identity events.

The builder has exclusive mutation authority over an owner-private construction directory. Writer receipts attest bytes produced under that authority. Concurrent external tampering is not silently declared impossible: `cdf package verify`, replay, and destination readers hash while consuming and compare the manifest; staged ingress acknowledges exact segment hashes. Finalization's removal of redundant self-rereads does not remove independent verification paths.

The draft index records each normalized path once, rejects duplicates/unregistered identity files, and sorts canonically with bounded memory/spill. Finalization reconciles filesystem path/type/size against receipts, streams canonical `ManifestIdentity` bytes to compute package hash, and streams the final manifest without materializing all entries/bytes. The first implementation MUST preserve manifest-v1 canonical bytes and hashes for unchanged fixtures; any necessary v2 is a separate decision.

Residual unregistered compatibility files may be hashed once in parallel at finalization during migration, are reported, and must disappear from production writers before WS-E closes. SHA-256 remains the artifact algorithm. Hardware acceleration/library flags are measured and supply-chain reviewed; algorithm change is not authorized.

Local replay/verification evaluates buffered read versus mmap. Mmap is adopted only with measured benefit, a focused unsafe decision/safety proof/fuzz target, and no lifetime/tamper regression. Otherwise ordinary streaming reads remain.

## Alternatives considered

- Keep rereads for simplicity: rejected because terabyte segments pay avoidable device bandwidth and serialization stalls.
- Skip fsync entirely: rejected because durable package/staging claims would be false.
- Sync only at package end: rejected for pre-final staged segments.
- Sync every event/file directory immediately: rejected because phase metadata has no visibility consumer before final manifest.
- Change to a faster hash now: rejected because SHA-256 is artifact identity and expected hardware rate should fit the budget after teeing.
- Mmap by default: rejected pending measurement and unsafe review.

## Consequences

Package builder APIs return receipts rather than reconstructing entries. A5 metadata sinks and WS-E share the draft index/finalizer. Verification becomes streaming/parallel where safe. Lab phase telemetry separates encode, write, hash, sync, manifest, verify, and consumer-read costs.
