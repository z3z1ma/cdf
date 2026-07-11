Status: open
Created: 2026-07-10
Updated: 2026-07-11
Parent: .10x/tickets/2026-07-10-p3-terabyte-scale-program.md
Depends-On: .10x/tickets/2026-07-10-p3-ws-l-performance-lab.md, .10x/tickets/2026-07-07-package-io-hashing-overhead-triage.md

# P3 WS-E: hashing and package I/O

## Scope

Hash segment bytes while writing, remove redundant full data-file rereads, parallelize residual small-file hashing, measure hardware SHA-256, coalesce directory fsyncs only within ratified durability semantics, and evaluate mmap for local replay.

## Activated children

- `.10x/tickets/2026-07-11-p3-e1-hashing-artifact-sink.md`
- `.10x/tickets/2026-07-11-p3-e2-streaming-manifest-durability.md`
- `.10x/tickets/2026-07-11-p3-e3-streaming-verification-replay-io.md`
- `.10x/tickets/2026-07-11-p3-e4-package-io-envelope.md`

## Acceptance criteria

- Manifest bytes and hashes remain identical for fixed fixtures.
- Segment data is not reopened solely to compute the hash already available during write.
- Package build reaches the envelope and hashing is at most 5% of wall time.
- Crash/atomicity tests prove no durability regression.

## Explicit exclusions

No hash algorithm or artifact-spec change without a separate active decision triggered by post-optimization evidence.

## Blockers

Blocked until WS-L baseline evidence exists.

## References

- `.10x/decisions/hash-while-write-and-durability-barriers.md`
- `.10x/specs/package-io-hashing-durability.md`
