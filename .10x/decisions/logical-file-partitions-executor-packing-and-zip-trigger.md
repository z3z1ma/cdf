Status: active
Created: 2026-07-10
Updated: 2026-07-10

# Logical file partitions, executor packing, and zip activation trigger

## Context

P2 asked for a large-N coalescing policy and an explicit zip decision. Treating either as a local file-reader shortcut would leak execution topology into manifest identity, retries, schema observations, and checkpoints. The effective-schema authority already requires package identity to remain invariant under executor partition packing.

## Decision

One resolved file remains one logical plan partition and one independently attestable `FileManifest` identity at every cardinality. CDF does not coalesce logical file partitions above a fixed file-count threshold.

Executors MAY pack multiple logical partitions into one worker task or scheduling lease. Packing is derived from byte/row estimates, memory-ledger availability, destination pressure, and configured jobs; it MUST preserve each logical partition id, observation binding, source position, retry result, and deterministic segment assignment. Packing policy and resolved budgets are execution evidence, not source semantics. P3 WS-A/WS-C own its bounded implementation and jobs-invariance law.

Zip archive ingestion is intentionally not implemented in P2. It activates only after all of these contracts exist:

1. a source-neutral archive-member position records outer object identity, canonical member path, compressed/uncompressed sizes, and CRC/checksum where available;
2. member globs and path traversal rejection are specified;
3. the P3 memory ledger accounts for central-directory reads, member decode buffers, spill, maximum member count, maximum expanded bytes, and compression-ratio limits;
4. remote seek/ranged-read versus bounded spool behavior is transport-independent;
5. conformance proves member-level retry, manifest incrementality, preview/run parity, and zip-bomb clean failure.

Until then, zip is a named unsupported format with remediation to extract to ordinary file/object partitions. This is a recorded no-action boundary, not an invitation for adapter-local archive handling.

## Alternatives considered

- Replace N small file partitions with one plan partition. Rejected because retry/checkpoint/schema identity would depend on a performance heuristic.
- Fixed files-per-task threshold. Rejected because 1,000 tiny files and 1,000 multi-gigabyte files require different scheduling while preserving identical logical semantics.
- Treat zip as transparent compression like gzip/zstd. Rejected because zip is an archive with member identity and random-access/security semantics, not one compressed byte stream.

## Consequences

P2 keeps complete per-file evidence and modest/large cardinality share one semantic model. P3 can optimize scheduling without changing plans or package identity. Zip has precise implementation prerequisites and cannot enter through a one-off format branch.
