Status: done
Created: 2026-07-11
Updated: 2026-07-11
Parent: .10x/tickets/done/2026-07-10-p3-ws-d-destination-bulk-paths.md
Depends-On: .10x/tickets/done/2026-07-11-p3-d1-bulk-path-contract.md, .10x/tickets/done/2026-07-11-p3-a1-staged-ingress-final-binding.md, .10x/tickets/done/2026-07-07-native-parquet-streaming-write-triage.md

# P3 D4: streaming Parquet/object-store writer

## Scope

Implement bounded row-group/file encoding as durable segments arrive, local atomic temp installation, remote multipart/temp-object staging, deterministic object/row-group policy, final manifest/pointer binding, and no full-table buffer.

## Acceptance criteria

- Peak writer memory is bounded by declared row groups/multipart buffers, independent of package size.
- Local/object-store outputs remain invisible until final binding and abort/cleanup/recovery are idempotent.
- Hashes, receipts, append/replace, duplicate replay, corrections, and jobs laws remain correct.
- Throughput reaches ≥60% device-write roofline; remote uploads overlap encoding/network.

## Evidence expectations

Row-group/file-size comparison, local/device and multipart profiles, high-file-count/object identity goldens, crash/cleanup/duplicate conformance, memory stress, and envelope report.

## Explicit exclusions

No Iceberg/Delta transaction protocol.

## Blockers

Depends on D1, staged ingress, and streaming writer triage.

## References

- `.10x/specs/destination-bulk-path-runtime.md`

## Progress and notes

- 2026-07-11: Replaced per-segment Parquet `Vec<u8>` materialization with a canonical 64k-row-group writer on the declared `parquet.encode` blocking lane. Encoding writes and hashes directly into a spill-accounted durable temp file while retaining the verified input segment and a destination writer memory lease. After encode, multipart upload uses 8 MiB chunks, up to four concurrent parts, asynchronous ledger admission/backpressure, atomic multipart completion, and abort on read/part failure. The path descriptor now reports the same live `arrow_ipc_to_parquet` id everywhere. Shared schema validation remains owned by `cdf-package` and is reused by both transcode and streaming encode. All 27 destination tests pass, including filesystem/object-store, duplicate, replace, correction, tamper, schema, and abort behavior. Evidence: `.10x/evidence/2026-07-11-p3-d4-streaming-parquet-milestone.md`.
- 2026-07-11: Removed local filesystem I/O amplification without branching generic orchestration. The store facade now declares local-install capability by retaining its root, creates encode staging files on that filesystem, and installs them with atomic no-clobber rename plus directory sync. Crash-orphan collisions stream-hash the existing immutable file and accept only exact identity. Remote/custom stores retain bounded multipart. The filesystem test proves successful commits leave the staging directory empty; all 27 destination tests and strict Clippy remain green.
- 2026-07-11: Added a release median-of-three local write-roofline law using 128 MiB of high-entropy Arrow values and equal physical-byte durable writes. The inherited ArrowWriter defaults measured 332.3 MiB/s against 1,659.9 MiB/s raw (0.200x). Compiling a bounded 1M-row/32 MiB row-group policy, 1M-row internal write batches, 64k-row/8 MiB pages, and disabling unneeded dictionary/statistics work produced repeat final medians of 1,028–1,104 MiB/s and ratios of 0.612–0.639, clearing the ≥60% local target with roughly 3.1–3.3x absolute improvement. The benchmark is permanent and ignored outside the release performance tier.
- 2026-07-11: Extended the package-side Parquet foundation to identity evidence: quarantine and dedup provenance no longer transcode into complete `Vec<u8>` artifacts. `StreamingIdentityArtifact` now implements the standard writer contract over its atomic hash-while-write sink, and an owned quarantine Parquet writer accepts successive bounded record chunks before one atomic finish/journal receipt. This is format-neutral package infrastructure, not destination-specific orchestration. Evidence: `.10x/evidence/2026-07-11-p3-v2-streaming-quarantine-evidence.md`.
- 2026-07-11: Closed D4 after removing the final session-equivalence test's `read_commit_segments()` materialization and exercising the same verified, leased segment stream as production. A fresh 128 MiB release median measured 1,006.8 MiB/s Parquet versus 1,569.3 MiB/s equal-byte durable raw writes (0.642x), above the 60% target. All 27 non-ignored local/object-store tests and strict all-target Clippy pass. The implementation bounds encoding by declared row-group/page/batch memory, uploads remote/custom objects through byte-admitted concurrent multipart, installs local objects atomically without copy amplification, and preserves manifest/pointer/receipt/correction/duplicate semantics. Evidence: `.10x/evidence/2026-07-11-p3-d4-parquet-closeout.md`; review: `.10x/reviews/2026-07-11-p3-d4-closeout-review.md`.
