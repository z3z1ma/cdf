Status: done
Created: 2026-07-10
Updated: 2026-07-19
Parent: .10x/tickets/2026-07-10-p3-terabyte-scale-program.md
Depends-On: .10x/tickets/done/2026-07-10-p3-ws-l-performance-lab.md, .10x/tickets/done/2026-07-07-package-io-hashing-overhead-triage.md

# P3 WS-E: hashing and package I/O

## Scope

Hash segment bytes while writing, remove redundant full data-file rereads, parallelize residual small-file hashing, measure hardware SHA-256, coalesce directory fsyncs only within ratified durability semantics, and evaluate mmap for local replay.

## Activated children

- `.10x/tickets/done/2026-07-11-p3-e1-hashing-artifact-sink.md`
- `.10x/tickets/done/2026-07-11-p3-e2-streaming-manifest-durability.md`
- `.10x/tickets/done/2026-07-11-p3-e3-streaming-verification-replay-io.md`
- `.10x/tickets/done/2026-07-11-p3-e4-package-io-envelope.md`

## Acceptance criteria

- Manifest bytes and hashes remain identical for fixed fixtures.
- Segment data is not reopened solely to compute the hash already available during write.
- Package build reaches the envelope and hashing is at most 5% of wall time.
- Crash/atomicity tests prove no durability regression.

## Explicit exclusions

No hash algorithm or artifact-spec change without a separate active decision triggered by post-optimization evidence.

## Blockers

None. E1-E4 are complete.

## References

- `.10x/decisions/hash-while-write-and-durability-barriers.md`
- `.10x/specs/package-io-hashing-durability.md`

## Journal

- 2026-07-19: Closed E4 on the dedicated EC2 host. The production LZ4 IPC writer sustained 1,033.8 MiB/s across alternating 32 GiB hash-on/hash-off samples, 0.903x the direct-I/O device roofline, with 0.06% attributable hashing wall. Alternative SHA backends tied and were removed. The completed E1-E4 chain now covers exact hash-while-write receipts, streaming bounded manifests, capability-rooted bounded verification/opened-object consumption, crash/golden parity, zero production rereads, million-entry construction/verification, and the sustained package envelope.

## Evidence

- E1: `.10x/tickets/done/2026-07-11-p3-e1-hashing-artifact-sink.md`
- E2: `.10x/tickets/done/2026-07-11-p3-e2-streaming-manifest-durability.md`
- E3: `.10x/tickets/done/2026-07-11-p3-e3-streaming-verification-replay-io.md`
- E4: `.10x/tickets/done/2026-07-11-p3-e4-package-io-envelope.md`
- Final envelope: `.10x/evidence/2026-07-19-p3-e4-package-io-envelope.md`

## Review

### Findings

None. Parent review reconciled every child criterion against its ticket evidence and inspected the final benchmark method. No product dependency, alternate hash path, mmap, legacy reread shim, fixed worker cap, or weakened durability behavior remains.

### Verdict

**pass**. All parent acceptance criteria are supported, and the workstream is terminal.

### Residual risk

A future host with sustained package storage above approximately 5.5 GiB/s may expose aggregate SHA throughput. That measured trigger belongs to a future host-specific optimization, not the current backlog.

## Retrospective

The workstream improved by deleting work as evidence sharpened: post-write rereads, DOM manifests, pathname reopens, unadmitted verifier workers, and candidate SHA backends all disappeared. The resulting path has one receipt authority, one bounded manifest path, one capability-rooted reopened verification path, and no compatibility surface.
