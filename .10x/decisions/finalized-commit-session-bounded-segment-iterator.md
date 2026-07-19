Status: active
Created: 2026-07-19
Updated: 2026-07-19
Supersedes: `.10x/decisions/superseded/commit-session-segment-write-api.md` where finalized-package sessions accept payload one segment call at a time, and `.10x/decisions/destination-ingress-protocol-capability-split.md` only where it preserves that finalized per-segment call shape.

# Finalized commit sessions consume one bounded segment iterator

## Context

The superseded synchronous MVP decision `.10x/decisions/superseded/commit-session-segment-write-api.md` required one `CommitSession::write_segment` call per canonical segment and explicitly anticipated replacement once benchmark and backpressure evidence justified a segment stream. P3 D16 provides that evidence: Postgres's current Arrow-to-binary encoder and server-inclusive control sustain `1,375,614` rows/s, while the full product reaches only `400,862` rows/s because 215 canonical segments force 215 PostgreSQL COPY protocol setup/trailer/finish cycles.

The synchronous `postgres` client represents an active COPY as a writer borrowing its client. Retaining that writer across independent `write_segment` calls would require a self-referential destination object or unsafe lifetime extension. Moving package iteration into a Postgres-specific generic-runtime branch would violate the destination extension boundary. Materializing the package would violate the bounded-reader contract.

## Decision

Finalized-package `CommitSession` has one payload method:

```rust
pub type CommitSegmentIterator =
    Box<dyn Iterator<Item = Result<CommitSegment>> + Send>;

pub trait CommitSession {
    fn apply_migrations(&mut self) -> Result<()>;
    fn write_segments(
        &mut self,
        segments: CommitSegmentIterator,
    ) -> Result<Vec<SegmentAck>>;
    fn finalize(self: Box<Self>) -> Result<Receipt>;
    fn abort(self: Box<Self>) -> Result<()>;
}
```

Generic finalized-package orchestration MUST construct the iterator from the verified package's memory-ledger-owned segment reader and transfer it exactly once to the destination session. The iterator owns its path, verification-derived segment order, and memory coordinator; each yielded segment retains only its accounted Arrow window and MUST be released before the next segment needs that memory. The iterator MUST NOT materialize the package.

The destination MAY keep one protocol-native package ingest operation open while consuming the iterator. It returns one exact `SegmentAck` per accepted segment after the package payload operation succeeds. `SegmentAck` continues to mean accepted into the still-open package session; only `finalize` returns durable settlement. Generic orchestration records each returned acknowledgement and independently verifies the final receipt before checkpoint commit.

The returned acknowledgement vector is bounded metadata proportional to the manifest segment count, not payload. A destination MUST reject missing, duplicate, unexpected, or mismatched segments and MUST roll back on iterator, encode, send, or acknowledgement failure. Generic orchestration MUST independently require one acknowledgement per requested segment, in canonical request order, with exact segment identity and logical row count before it records acknowledgement events. Destination-physical byte counts remain receipt-verification data because they need not equal canonical package bytes.

This decision applies only to `DestinationIngress::FinalizedPackage`. `StagedSegmentIngress` retains its pre-finalization durable-segment lifecycle, acknowledgements, lease authority, and streaming API.

## Alternatives considered

Retain per-segment calls and accept the Postgres cost.

Rejected by measured macro evidence: the protocol lifecycle, not the encoder, leaves the full product roughly 3.4x below the server-inclusive binary COPY control.

Keep a PostgreSQL COPY writer across calls with a self-referential wrapper or transmuted lifetime.

Rejected. It spends unsafe complexity to preserve an API shape whose granularity is the measured defect.

Add a Postgres package-ingest branch to project replay.

Rejected. Destination protocol setup belongs behind the destination session trait; orchestration knows only verified segments, capabilities, acknowledgements, receipts, and abort.

Adopt an async stream and migrate every destination/runtime boundary now.

Rejected for this bounded synchronous finalized-package path. The owned iterator crosses the blocking-lane boundary without borrowing project state and solves the measured problem. Future async finalized destinations may supersede this spelling with evidence; staged ingress already covers pre-finalization asynchronous flow.

Return acknowledgements through a callback while COPY remains active.

Rejected. The finalized Postgres session runs in a blocking lane and acknowledgement callbacks would either borrow runtime state across that lane or leak event infrastructure into the destination. Returning bounded metadata after COPY succeeds keeps the boundary typed and rollback-safe.

## Consequences

Postgres can open one binary COPY, consume and release every verified canonical segment in order, finish the protocol once, then write segment-range mirrors and complete the existing package transaction. Other finalized destinations can amortize package-scoped setup without new orchestration branches. The superseded per-segment finalized method and its tests are deleted; there is one happy path.

Crash observation moves from individual synchronous call returns to the completed package-payload boundary for finalized sessions. This does not weaken durability: prior segment acknowledgements were process-local acceptance into the same uncommitted package transaction, and a crash before `finalize` already required rollback/redrive. Staged destinations retain externally durable per-segment acknowledgement where that distinction matters.
