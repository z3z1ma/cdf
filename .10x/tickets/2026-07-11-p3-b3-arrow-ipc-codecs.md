Status: open
Created: 2026-07-11
Updated: 2026-07-11
Parent: .10x/tickets/2026-07-10-p3-ws-b-format-decode-engines.md
Depends-On: .10x/tickets/done/2026-07-10-p3-ws-l5-preoptimization-baseline.md, .10x/tickets/2026-07-11-p0-fx1-native-format-extension-boundary.md

# P3 B3: Arrow IPC file and stream codecs

## Scope

Implement separate file/stream drivers with bounded batch yields, remote range/seek and spool fallback for file framing, sequential stream framing, dictionary/compression support, and discovery/runtime parity.

## Acceptance criteria

- Remote Arrow IPC file no longer hard-rejects solely for being remote.
- No IPC path collects every record batch; dictionaries/order/schema remain exact.
- File/stream framing mismatch, truncation, continuation, compression, and schema changes fail with precise evidence.
- Throughput/reference ratio and zero-copy/copy counts are recorded.

## Evidence expectations

Arrow reference comparison, remote range/spool fixtures, malformed/fuzz corpus, memory, schema/dictionary goldens, and profiles.

## Explicit exclusions

No subprocess protocol changes.

## Blockers

Depends on L5 and FX1.

## References

- `.10x/specs/native-enterprise-format-catalog.md`
- `.10x/specs/native-format-codec-runtime.md`

## Progress and notes

- 2026-07-11: Added the parser-local `cdf-format-arrow-ipc` file driver and registered it at composition roots. File discovery is footer-bounded; execution plans one deterministic file unit, reads dictionary and record-batch blocks by exact extents, preserves schema/custom metadata, supports exact projection, and emits accounted batches whose Arrow buffers retain source leases. Local and remote verified-spool execution now share the driver, and the former remote Arrow IPC hard rejection and file-source-local IPC execution branch were deleted. The release-mode in-memory construction comparison measured 8,196.51 MiB/s for Arrow's high-level `FileReader` and 471,540.98 MiB/s for the owner-backed driver (57.529x); this intentionally biased measurement demonstrates elimination of the high-level reader's block-buffer copy and is not a storage-throughput claim. Stream framing, compression/malformed/fuzz expansion, and storage-backed throughput evidence remain open. Evidence: `.10x/evidence/2026-07-11-p3-b3-native-arrow-ipc-file-driver.md`.
