Status: recorded
Created: 2026-07-11
Updated: 2026-07-11
Target: .10x/tickets/done/2026-07-11-p3-e2-streaming-manifest-durability.md
Verdict: pass

# P3 E2 adversarial closure review

## Target

Package draft metadata, trace durability, identity reconciliation/hash/serialization, atomic streaming evidence artifacts, and persisted archive generation.

## Findings

- No critical or significant correctness finding remained. Canonical v1 package hashes and serialized fixtures are unchanged, including manifests with archive metadata.
- Builder runtime metadata is journal-backed. Finalization keeps only the v1 file/segment vectors that are themselves the returned artifact model; it no longer duplicates them into directory-path vectors, receipt maps plus cloned entries, a JSON DOM, or a whole-manifest byte buffer.
- One million file entries hash at 4.44 million entries/s with about 176 MB maximum RSS including the owned million path/hash strings, zero page faults, and zero swaps. This is below the default 4 GiB ledger by more than an order of magnitude.
- Registered content is never reopened for hashing. Trace and all package writers carry hashing receipts, and an unregistered identity file aborts finalization with a named error.
- Trace ordering remains mutex-serialized, while durability is one phase flush/file-sync/directory barrier instead of per-line open/fsync/directory-sync.
- Atomic failure injection covers every publication boundary. Streaming evidence sinks delete unfinished temporary siblings; the manifest is published only after identity artifacts complete.
- Persisted archive generation holds one decoded segment plus its encoded sidecar, not the whole package. Force replacement verifies canonical identity while intentionally disregarding the damaged prior archive projection.

## Residual risk

The v1 public `PackageManifest` necessarily owns its final metadata vectors, so memory scales with the artifact's own metadata payload. A future manifest-v2 lazy index could remove that base cost, but v2 is explicitly excluded. E4 owns filesystem roofline, many-small-file syscall measurement, and verification/replay throughput; residual external-sort intermediate-file reclamation is an E4 optimization opportunity, not an E2 correctness gap.

## Verdict

Pass. E2's bounded working-set, canonical-v1, registered-writer, trace, and crash-publication contracts are satisfied.
