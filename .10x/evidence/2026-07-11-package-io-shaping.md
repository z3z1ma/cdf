Status: recorded
Created: 2026-07-11
Updated: 2026-07-11
Relates-To: .10x/decisions/hash-while-write-and-durability-barriers.md, .10x/specs/package-io-hashing-durability.md, .10x/tickets/done/2026-07-10-p3-ws-e-hashing-package-io.md

# Package I/O shaping evidence

## What was observed

Segments are reread once immediately after write and again with all identity files at finish. Small artifacts are reread after writing. Trace fsyncs file and package directory per event. Finalization/verification/read paths collect metadata and batches in vectors/maps.

## Procedure

Traced package create/write/trace/finish/status/read/verify/archive functions and their exact open/hash/sync/rename order, then reconciled with lifecycle and staged-ingress durability requirements.

## What this supports

Hashing writer receipts, named durability classes, phase barrier coalescing, bounded draft indexes/manifest serialization, and hash-during-consumer reads.

## Limits

This is source-backed shaping evidence. Filesystem and hardware benefits require L5/E1-E4 measurements.
