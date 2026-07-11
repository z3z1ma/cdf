Status: open
Created: 2026-07-11
Updated: 2026-07-11
Parent: .10x/tickets/2026-07-10-p3-ws-e-hashing-package-io.md
Depends-On: .10x/tickets/2026-07-11-p3-e1-hashing-artifact-sink.md, .10x/tickets/done/2026-07-11-p3-a2-unified-memory-ledger.md, .10x/tickets/2026-07-11-p3-a5-streaming-operator-graph.md

# P3 E2: bounded draft index, trace, and streaming manifest finalizer

## Scope

Implement append/spill-backed file/segment indexes, filesystem reconciliation, bounded trace sink, phase directory barriers, canonical streaming v1 identity/manifest serialization, and eliminate finish-time rehash/metadata vectors.

## Acceptance criteria

- Existing package fixtures remain byte/hash-identical at manifest v1.
- Finalization holds bounded memory for million-entry synthetic manifests.
- Registered content is not reread; unregistered production writers are zero at closure.
- Trace ordering/bytes remain deterministic while per-line fsync/directory sync disappears.
- Crash injection proves every segment/metadata/manifest visibility boundary.

## Evidence expectations

Million-entry RSS test, filesystem/syscall traces, golden packages, unexpected/symlink/size mutation tests, crash matrix, trace parity, and before/after many-small-segment benchmark.

## Explicit exclusions

No manifest v2 or lifecycle semantic change.

## Blockers

Depends on E1, memory ledger, and A5 metadata sink integration.

## References

- `.10x/specs/package-io-hashing-durability.md`
