Status: done
Created: 2026-07-11
Updated: 2026-07-11

# Package I/O, hashing, and durability audit

## Question

How can package construction eliminate redundant reads/syncs, remain constant-memory at high file cardinality, and preserve atomicity, identity, staged-segment durability, replay verification, and current golden bytes?

## Sources and methods

Inspected package storage/builder/model/reader/verify/archive paths, lifecycle/determinism spec, staged-ingress and streaming graph contracts, hashing triage, trace writes, manifest/status updates, and current IPC writer ordering.

## Findings

The segment path writes IPC to a temp file, fsyncs, renames, syncs the data directory, then reopens and hashes the complete segment to create `SegmentEntry`. At package finish, directory traversal reopens and hashes every identity file—including every segment—again. Data bytes are therefore reread twice after their original write before any explicit verification/destination read.

Every non-segment identity artifact is atomically written from bytes already in memory, then reopened and hashed. `append_trace_event` opens/appends/syncs the trace file and syncs the package directory for every line. Package creation/status/finalization atomically rewrite `manifest.json`; finalization holds file and segment vectors and serializes complete manifest bytes in memory.

`collect_identity_file_entries` recursively collects/sorts all paths and hashes sequentially. Verification also builds file maps/vectors and rereads all bytes. Package readers load the complete manifest and segment files into vectors. A5 owns bounded readers/metadata sinks, but WS-E must supply writer/finalizer primitives compatible with them.

The current crash contract treats pre-packaged drafts as garbage/recoverable source work. Pre-finalization staged ingress strengthens only durable segment publication: segment bytes/hash/directory entry must be complete before staging. Other package metadata must be durable before final manifest publication, not after every individual write. Trace is identity evidence at finalization but is not the run-ledger recovery authority.

Hash-while-write can return exact bytes/count/SHA from the stream that created the file. For in-memory canonical JSON bytes, hashing can occur before/during write. Finalization can consume these trusted writer receipts and reconcile the directory/path/size inventory without rereading content. Explicit package verification and destination/replay segment reads continue to hash/compare while consuming bytes.

The trust boundary must be explicit: writer receipts prove bytes emitted by the exclusive package builder, not resistance to a concurrent external writer mutating package files between atomic install and finalization. Package directories therefore require exclusive builder ownership and owner-only mutation during construction; explicit verify remains the tamper check. Final binding consumes verified segment streams/acks under staged-ingress rules.

## Conclusion

Introduce a hashing/atomic `ArtifactSink` returning a typed `WrittenArtifact` receipt and durability state. Maintain append/spill-backed file/segment draft indexes. Finalization streams canonical v1-compatible manifest identity/output from sorted draft entries, hashes identity incrementally, and does not content-rehash writer-owned files.

Use named durability barriers: durable segment publication syncs file and directory before staging; metadata/trace writes may coalesce directory syncs until phase/final manifest barriers; the final manifest is fsynced/renamed and package directory synced last. Hash segment bytes again only when an independent consumer/verify pass actually reads them.

## Limits

L5/WS-E must measure SHA hardware rate, fsync cost by filesystem, manifest cardinality, and mmap/read strategies. Any manifest byte/version change requires its own artifact migration; the preferred first implementation preserves v1 canonical bytes through streaming serialization.
