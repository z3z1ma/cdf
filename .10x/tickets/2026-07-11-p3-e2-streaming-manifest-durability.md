Status: open
Created: 2026-07-11
Updated: 2026-07-11
Parent: .10x/tickets/2026-07-10-p3-ws-e-hashing-package-io.md
Depends-On: .10x/tickets/done/2026-07-11-p3-e1-hashing-artifact-sink.md, .10x/tickets/done/2026-07-11-p3-a2-unified-memory-ledger.md, .10x/tickets/done/2026-07-11-p3-a5a-graph-edge-contracts.md

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

None. E1, the memory ledger, and the A5 graph-edge ownership contracts are complete. This ticket supplies the bounded metadata sink required by A5b/A5e; depending on the A5 parent would be circular.

## References

- `.10x/specs/package-io-hashing-durability.md`

## Progress and notes

- 2026-07-11: Replaced per-event trace open/write/fsync/directory-sync with one ordered mutex-owned sink and a single flush/file-sync/directory barrier at package finalization. Existing package and trace goldens remain unchanged.
- 2026-07-11: Replaced the builder's cardinality-proportional in-memory segment draft vector and artifact receipt map with append-only temporary journals. Runtime draft metadata no longer grows in the builder; finalization reconstructs only the v1 artifact shape required by the existing public return contract.
- 2026-07-11: Replaced manifest identity DOM materialization and whole-manifest byte buffering with canonical streaming encoders into SHA-256 and the atomic manifest sink. The v1 fixed fixture hash and archive-bearing manifest tests remain byte-identical. Specialized file/segment entry encoders avoid one `serde_json::Value` tree per entry.
- 2026-07-11: Release evidence for a one-million-file identity: 225,134,083 ns serialization (4,441,797 entries/s), 175,800,320-byte process maximum RSS including the million owned path/hash strings, zero page faults, and zero swaps. Remaining closure work is filesystem reconciliation without finish-time path vectors, archive metadata streaming, crash injection at finalization boundaries, and the many-small-segment syscall benchmark.
