Status: open
Created: 2026-07-11
Updated: 2026-07-11
Parent: .10x/tickets/2026-07-10-p3-ws-d-destination-bulk-paths.md
Depends-On: .10x/tickets/done/2026-07-11-p3-d1-bulk-path-contract.md, .10x/tickets/done/2026-07-11-p3-a1-staged-ingress-final-binding.md, .10x/tickets/2026-07-07-native-parquet-streaming-write-triage.md

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
